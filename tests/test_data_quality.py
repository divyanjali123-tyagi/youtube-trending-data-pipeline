"""Data quality rules and Lambda handler."""

import json
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

import bronze_to_silver as b2s
import dq_handler
import ingest_handler as ih
import rules
import silver_to_gold as s2g
from fake_youtube_api import FakeYouTubeApi
from run_local import SparkQueryRunner

PARAMS = {**rules.table_params(), "date": "2026-10-25", "hour": "09", "expected_regions": 10, "min_regions": 8}


class StubRunner:
    def __init__(self, values):
        self.values = values
        self.queries = {}

    def run_many(self, queries):
        self.queries = queries
        return {name: self.values.get(name, 0) for name in queries}


def test_rule_evaluation_and_placeholder_thresholds():
    rule = next(r for r in rules.SILVER_RULES if r.name == "silver_min_region_coverage")
    assert rule.evaluate(9, PARAMS)["passed"]
    assert not rule.evaluate(7, PARAMS)["passed"]
    assert not rule.evaluate(None, PARAMS)["passed"]


def test_overall_status():
    ok = {"passed": True, "severity": rules.CRITICAL}
    warn = {"passed": False, "severity": rules.WARNING}
    crit = {"passed": False, "severity": rules.CRITICAL}
    assert rules.overall_status([ok]) == "PASSED"
    assert rules.overall_status([ok, warn]) == "PASSED_WITH_WARNINGS"
    assert rules.overall_status([warn, crit]) == "FAILED"


def test_rendered_sql_targets_partition_and_database():
    params = {**PARAMS, **rules.table_params("youtube_trending")}
    sql = rules.SILVER_RULES[0].render(params)
    assert '"youtube_trending".silver_trending_videos' in sql
    assert "trending_date = '2026-10-25' AND ingest_hour = '09'" in sql


def test_handler_gates_on_critical_failure(monkeypatch):
    monkeypatch.delenv("BUCKET", raising=False)
    values = {"silver_row_count": 500, "silver_min_region_coverage": 10, "silver_all_regions_present": 10}
    event = {"suite": "silver", "trending_date": "2026-10-25", "ingest_hour": "09"}

    assert dq_handler.lambda_handler(event, runner=StubRunner(values))["status"] == "PASSED"

    values["silver_unique_video_region"] = 3
    out = dq_handler.lambda_handler(event, runner=StubRunner(values))
    assert out["status"] == "FAILED" and out["checks_failed"] == ["silver_unique_video_region"]


def test_handler_rejects_unknown_suite():
    with pytest.raises(ValueError):
        dq_handler.lambda_handler({"suite": "bronze", "trending_date": "x"}, runner=StubRunner({}))


class FakeAthena:
    def __init__(self):
        self.started = []
        self.polls = 0

    def start_query_execution(self, **kw):
        self.started.append(kw)
        return {"QueryExecutionId": f"q{len(self.started)}"}

    def get_query_execution(self, QueryExecutionId):
        self.polls += 1
        state = "RUNNING" if self.polls <= 2 else "SUCCEEDED"
        return {"QueryExecution": {"Status": {"State": state}}}

    def get_query_results(self, QueryExecutionId, MaxResults):
        return {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": "value"}]}, {"Data": [{"VarCharValue": "42"}]}]}}


def test_athena_runner_runs_queries_in_parallel_and_polls():
    athena = FakeAthena()
    runner = dq_handler.AthenaQueryRunner("db", "wg", client=athena, sleep=lambda s: None)
    out = runner.run_many({"a": "SELECT 1", "b": "SELECT 2"})
    assert out == {"a": 42.0, "b": 42.0}
    assert {s["WorkGroup"] for s in athena.started} == {"wg"}
    assert athena.started[0]["QueryExecutionContext"] == {"Database": "db"}


def test_athena_runner_surfaces_failed_queries():
    class Failing(FakeAthena):
        def get_query_execution(self, QueryExecutionId):
            return {"QueryExecution": {"Status": {"State": "FAILED", "StateChangeReason": "SYNTAX_ERROR"}}}

    with pytest.raises(RuntimeError, match="SYNTAX_ERROR"):
        dq_handler.AthenaQueryRunner("db", "wg", client=Failing()).run_many({"a": "x"})


@mock_aws
def test_results_written_to_s3(monkeypatch):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="lake")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("BUCKET", "lake")
    event = {"suite": "gold", "trending_date": "2026-10-25", "ingest_hour": "09", "run_id": "r1"}
    out = dq_handler.lambda_handler(event, runner=StubRunner({}))
    key = "dq/results/suite=gold/trending_date=2026-10-25/ingest_hour=09/r1.jsonl"
    assert out["results_location"] == f"s3://lake/{key}"
    lines = s3.get_object(Bucket="lake", Key=key)["Body"].read().decode().splitlines()
    assert len(lines) == len(rules.GOLD_RULES)
    assert json.loads(lines[0])["run_id"] == "r1"


# ----------------------------------------------------- rules on real data (Spark SQL)


@pytest.fixture(scope="module")
def lake(tmp_path_factory, spark):
    root = tmp_path_factory.mktemp("dq_lake")
    now = datetime(2026, 10, 25, 9, tzinfo=timezone.utc)
    c = ih.build_run_context({"run_id": "r", "execution_start": now.isoformat()})
    regions = ih.DEFAULT_REGIONS.split(",")
    ih.ingest(regions, "k", ih.LocalStorage(root), c, http_get=FakeYouTubeApi(now=now, videos_per_region=20))
    b2s.run(spark, str(root), "2026-10-25", "09")
    s2g.run(spark, str(root), "2026-10-25")
    return str(root)


@pytest.mark.parametrize("suite", ["silver", "gold"])
def test_all_rules_pass_on_clean_pipeline_output(spark, lake, suite):
    results = dq_handler.run_suite(suite, SparkQueryRunner(spark, lake), PARAMS)
    failed = [r for r in results if not r["passed"]]
    assert failed == []


def test_rules_catch_corrupted_silver(spark, lake):
    from pyspark.sql import functions as F

    silver = spark.read.parquet(f"{lake}/silver/trending_videos")
    bad = silver.union(silver.limit(5)).withColumn(
        "view_count", F.when(F.col("trending_rank") == 1, F.lit(-1)).otherwise(F.col("view_count"))
    )
    bad.createOrReplaceTempView("silver_trending_videos")
    runner = SparkQueryRunner.__new__(SparkQueryRunner)
    runner.spark = spark
    spark.read.parquet(f"{lake}/silver/quarantine_trending_videos").createOrReplaceTempView(
        "silver_quarantine_trending_videos"
    )
    results = {r["check_name"]: r for r in dq_handler.run_suite("silver", runner, PARAMS)}
    assert not results["silver_unique_video_region"]["passed"]
    assert not results["silver_non_negative_metrics"]["passed"]
    assert rules.overall_status(list(results.values())) == "FAILED"
