"""Declarative data quality rules for the Silver and Gold layers.

Each rule is a SQL query that returns a single numeric ``value`` plus an assertion
(operator + threshold). The SQL is deliberately portable - it runs unchanged on
Amazon Athena (Trino) in AWS and on Spark SQL in the local runner/tests.

Placeholders filled at run time:
    {silver} {quarantine} {gold_videos} {gold_kpis} {gold_categories}
    {gold_channels} {gold_cross_region}   -> fully-qualified table names
    {date} {hour}                           -> partition being validated
    {min_regions} {expected_regions}        -> region coverage thresholds

Severity:
    critical -> failing check stops the pipeline (Step Functions Fail state + alert)
    warning  -> recorded and alerted in the results, pipeline continues
"""

from __future__ import annotations

import operator
from dataclasses import dataclass

CRITICAL = "critical"
WARNING = "warning"

OPERATORS = {
    "==": operator.eq,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
}


@dataclass(frozen=True)
class Rule:
    name: str
    suite: str
    severity: str
    description: str
    sql: str
    op: str
    threshold: float | str  # numeric, or a placeholder name such as "min_regions"

    def render(self, params: dict) -> str:
        return " ".join(self.sql.format(**params).split())

    def resolve_threshold(self, params: dict) -> float:
        if isinstance(self.threshold, str):
            return float(params[self.threshold])
        return float(self.threshold)

    def evaluate(self, value: float | None, params: dict) -> dict:
        threshold = self.resolve_threshold(params)
        passed = value is not None and OPERATORS[self.op](float(value), threshold)
        return {
            "check_name": self.name,
            "suite": self.suite,
            "severity": self.severity,
            "description": self.description,
            "value": None if value is None else float(value),
            "operator": self.op,
            "threshold": threshold,
            "passed": bool(passed),
        }


SILVER_WHERE = "trending_date = '{date}' AND ingest_hour = '{hour}'"
GOLD_WHERE = "trending_date = '{date}'"

SILVER_RULES = [
    Rule(
        "silver_row_count",
        "silver",
        CRITICAL,
        "Run produced at least one clean record",
        f"SELECT COUNT(*) AS value FROM {{silver}} WHERE {SILVER_WHERE}",
        ">",
        0,
    ),
    Rule(
        "silver_min_region_coverage",
        "silver",
        CRITICAL,
        "Enough regions ingested successfully",
        f"SELECT COUNT(DISTINCT region) AS value FROM {{silver}} WHERE {SILVER_WHERE}",
        ">=",
        "min_regions",
    ),
    Rule(
        "silver_all_regions_present",
        "silver",
        WARNING,
        "Every configured region is present",
        f"SELECT COUNT(DISTINCT region) AS value FROM {{silver}} WHERE {SILVER_WHERE}",
        ">=",
        "expected_regions",
    ),
    Rule(
        "silver_null_keys",
        "silver",
        CRITICAL,
        "No null primary-key columns",
        "SELECT SUM(CASE WHEN video_id IS NULL OR region IS NULL OR run_id IS NULL THEN 1 ELSE 0 END) "
        f"AS value FROM {{silver}} WHERE {SILVER_WHERE}",
        "==",
        0,
    ),
    Rule(
        "silver_unique_video_region",
        "silver",
        CRITICAL,
        "(video_id, region) is unique within a run",
        "SELECT COUNT(*) - COUNT(DISTINCT CONCAT(video_id, '|', region)) AS value "
        f"FROM {{silver}} WHERE {SILVER_WHERE}",
        "==",
        0,
    ),
    Rule(
        "silver_non_negative_metrics",
        "silver",
        CRITICAL,
        "View/like/comment counts are never negative",
        "SELECT SUM(CASE WHEN view_count < 0 OR like_count < 0 OR comment_count < 0 THEN 1 ELSE 0 END) "
        f"AS value FROM {{silver}} WHERE {SILVER_WHERE}",
        "==",
        0,
    ),
    Rule(
        "silver_rank_in_range",
        "silver",
        CRITICAL,
        "Trending rank is between 1 and 200",
        "SELECT SUM(CASE WHEN trending_rank < 1 OR trending_rank > 200 THEN 1 ELSE 0 END) "
        f"AS value FROM {{silver}} WHERE {SILVER_WHERE}",
        "==",
        0,
    ),
    Rule(
        "silver_likes_not_above_views",
        "silver",
        WARNING,
        "Likes do not exceed views",
        "SELECT SUM(CASE WHEN like_count > view_count THEN 1 ELSE 0 END) "
        f"AS value FROM {{silver}} WHERE {SILVER_WHERE}",
        "==",
        0,
    ),
    Rule(
        "silver_category_enrichment_rate",
        "silver",
        WARNING,
        "Share of videos without a category name is at most 5%",
        "SELECT AVG(CASE WHEN category_name IS NULL THEN 1.0 ELSE 0.0 END) "
        f"AS value FROM {{silver}} WHERE {SILVER_WHERE}",
        "<=",
        0.05,
    ),
    Rule(
        "silver_quarantine_rate",
        "silver",
        WARNING,
        "At most 10% of raw records were quarantined",
        f"SELECT CAST(q.n AS DOUBLE) / (q.n + s.n) AS value FROM "
        f"(SELECT COUNT(*) AS n FROM {{quarantine}} WHERE {SILVER_WHERE}) q CROSS JOIN "
        f"(SELECT COUNT(*) AS n FROM {{silver}} WHERE {SILVER_WHERE}) s",
        "<=",
        0.10,
    ),
]

GOLD_RULES = [
    Rule(
        "gold_video_reconciliation",
        "gold",
        CRITICAL,
        "Gold daily videos reconcile 1:1 with distinct Silver (video, region) pairs",
        f"SELECT g.n - s.n AS value FROM "
        f"(SELECT COUNT(*) AS n FROM {{gold_videos}} WHERE {GOLD_WHERE}) g CROSS JOIN "
        f"(SELECT COUNT(DISTINCT CONCAT(video_id, '|', region)) AS n FROM {{silver}} WHERE {GOLD_WHERE}) s",
        "==",
        0,
    ),
    Rule(
        "gold_kpis_one_row_per_region",
        "gold",
        CRITICAL,
        "Exactly one KPI row per region present in Silver",
        f"SELECT g.n - s.n AS value FROM "
        f"(SELECT COUNT(*) AS n FROM {{gold_kpis}} WHERE {GOLD_WHERE}) g CROSS JOIN "
        f"(SELECT COUNT(DISTINCT region) AS n FROM {{silver}} WHERE {GOLD_WHERE}) s",
        "==",
        0,
    ),
    Rule(
        "gold_category_shares_sum_to_one",
        "gold",
        CRITICAL,
        "Category shares sum to 1 for every region",
        "SELECT MAX(ABS(total - 1.0)) AS value FROM (SELECT region, SUM(share_of_videos) AS total "
        f"FROM {{gold_categories}} WHERE {GOLD_WHERE} GROUP BY region) t",
        "<=",
        0.001,
    ),
    Rule(
        "gold_leaderboard_rank_unique",
        "gold",
        CRITICAL,
        "Leaderboard ranks are unique per region",
        "SELECT COUNT(*) - COUNT(DISTINCT CONCAT(region, '|', CAST(leaderboard_rank AS VARCHAR(10)))) "
        f"AS value FROM {{gold_channels}} WHERE {GOLD_WHERE}",
        "==",
        0,
    ),
    Rule(
        "gold_cross_region_bounds",
        "gold",
        CRITICAL,
        "regions_count is between 1 and the number of configured regions",
        "SELECT SUM(CASE WHEN regions_count < 1 OR regions_count > {expected_regions} THEN 1 ELSE 0 END) "
        f"AS value FROM {{gold_cross_region}} WHERE {GOLD_WHERE}",
        "==",
        0,
    ),
    Rule(
        "gold_view_gain_non_negative",
        "gold",
        WARNING,
        "Intraday view gain is not negative (views should only grow)",
        f"SELECT SUM(CASE WHEN view_gain < 0 THEN 1 ELSE 0 END) AS value FROM {{gold_videos}} WHERE {GOLD_WHERE}",
        "==",
        0,
    ),
]

SUITES = {"silver": SILVER_RULES, "gold": GOLD_RULES}

TABLES = {
    "silver": "silver_trending_videos",
    "quarantine": "silver_quarantine_trending_videos",
    "gold_videos": "gold_daily_video_trending",
    "gold_kpis": "gold_region_daily_kpis",
    "gold_categories": "gold_region_category_daily",
    "gold_channels": "gold_channel_daily_leaderboard",
    "gold_cross_region": "gold_cross_region_daily",
}


def table_params(database: str | None = None) -> dict[str, str]:
    prefix = f'"{database}".' if database else ""
    return {key: f"{prefix}{name}" for key, name in TABLES.items()}


def overall_status(results: list[dict]) -> str:
    failed = [r for r in results if not r["passed"]]
    if any(r["severity"] == CRITICAL for r in failed):
        return "FAILED"
    if failed:
        return "PASSED_WITH_WARNINGS"
    return "PASSED"
