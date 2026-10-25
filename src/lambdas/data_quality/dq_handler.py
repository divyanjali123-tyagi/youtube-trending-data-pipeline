"""Data quality Lambda: runs a rule suite against Silver or Gold through Athena.

Invoked by Step Functions after each Glue job with, e.g.::

    {"suite": "silver", "trending_date": "2026-10-25", "ingest_hour": "09", "run_id": "..."}

All rule queries are started in parallel, polled to completion, evaluated and the
results are written to ``dq/results/`` (queryable as the ``dq_results`` Athena
table). The returned ``status`` drives the Step Functions quality gate:
PASSED | PASSED_WITH_WARNINGS continue, FAILED stops the pipeline.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Protocol

from rules import SUITES, overall_status, table_params

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class QueryRunner(Protocol):
    def run_many(self, queries: dict[str, str]) -> dict[str, float | None]: ...


class AthenaQueryRunner:
    """Runs scalar queries on Athena concurrently and returns ``{name: value}``."""

    def __init__(
        self,
        database: str,
        workgroup: str,
        client=None,
        poll_seconds: float = 1.0,
        timeout_seconds: float = 240,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if client is None:
            import boto3

            client = boto3.client("athena")
        self.client = client
        self.database = database
        self.workgroup = workgroup
        self.poll_seconds = poll_seconds
        self.timeout_seconds = timeout_seconds
        self.sleep = sleep

    def run_many(self, queries: dict[str, str]) -> dict[str, float | None]:
        ids = {
            name: self.client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Database": self.database},
                WorkGroup=self.workgroup,
            )["QueryExecutionId"]
            for name, sql in queries.items()
        }
        pending = dict(ids)
        waited = 0.0
        while pending:
            for name, qid in list(pending.items()):
                status = self.client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
                state = status["State"]
                if state == "SUCCEEDED":
                    pending.pop(name)
                elif state in {"FAILED", "CANCELLED"}:
                    reason = status.get("StateChangeReason", "unknown error")
                    raise RuntimeError(f"Athena query for {name} {state}: {reason}")
            if pending:
                if waited >= self.timeout_seconds:
                    raise TimeoutError(f"Athena queries timed out: {sorted(pending)}")
                self.sleep(self.poll_seconds)
                waited += self.poll_seconds
        return {name: self._scalar(qid) for name, qid in ids.items()}

    def _scalar(self, query_id: str) -> float | None:
        rows = self.client.get_query_results(QueryExecutionId=query_id, MaxResults=2)["ResultSet"]["Rows"]
        if len(rows) < 2:
            return None
        cell = rows[1]["Data"][0]
        raw = cell.get("VarCharValue")
        return None if raw in (None, "") else float(raw)


def run_suite(suite: str, runner: QueryRunner, params: dict) -> list[dict]:
    rules = SUITES[suite]
    values = runner.run_many({rule.name: rule.render(params) for rule in rules})
    return [rule.evaluate(values.get(rule.name), params) for rule in rules]


def build_params(event: dict, database: str | None) -> dict:
    expected = int(os.getenv("EXPECTED_REGIONS", "10"))
    return {
        **table_params(database),
        "date": event["trending_date"],
        "hour": event.get("ingest_hour", "00"),
        "expected_regions": expected,
        "min_regions": int(os.getenv("MIN_REGIONS", str(max(1, expected - 2)))),
    }


def write_results(results: list[dict], event: dict, suite: str, s3_client=None) -> str | None:
    bucket = os.getenv("BUCKET")
    if not bucket:
        return None
    if s3_client is None:
        import boto3

        s3_client = boto3.client("s3")
    checked_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    run_id = event.get("run_id", "manual")
    lines = [json.dumps({**r, "run_id": run_id, "checked_at": checked_at}) for r in results]
    key = (
        f"dq/results/suite={suite}/trending_date={event['trending_date']}/"
        f"ingest_hour={event.get('ingest_hour', '00')}/{run_id}.jsonl"
    )
    s3_client.put_object(Bucket=bucket, Key=key, Body=("\n".join(lines) + "\n").encode())
    return f"s3://{bucket}/{key}"


def lambda_handler(event: dict, context: object = None, runner: QueryRunner | None = None) -> dict:
    suite = event["suite"]
    if suite not in SUITES:
        raise ValueError(f"Unknown suite {suite!r}; expected one of {sorted(SUITES)}")

    database = os.getenv("GLUE_DATABASE")
    runner = runner or AthenaQueryRunner(database=database, workgroup=os.environ["ATHENA_WORKGROUP"])
    results = run_suite(suite, runner, build_params(event, database))
    status = overall_status(results)
    location = write_results(results, event, suite)

    failed = [r["check_name"] for r in results if not r["passed"]]
    for r in results:
        logger.info(
            "[%s] %-34s %-8s value=%s %s %s",
            "PASS" if r["passed"] else "FAIL",
            r["check_name"],
            r["severity"],
            r["value"],
            r["operator"],
            r["threshold"],
        )
    return {
        "suite": suite,
        "status": status,
        "trending_date": event["trending_date"],
        "ingest_hour": event.get("ingest_hour"),
        "checks_total": len(results),
        "checks_failed": failed,
        "results_location": location,
    }
