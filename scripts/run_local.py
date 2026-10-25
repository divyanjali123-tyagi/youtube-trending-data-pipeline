"""Run the whole Medallion pipeline on your machine - no AWS account needed.

    python scripts/run_local.py                 # synthetic data, 2 intraday runs
    python scripts/run_local.py --live          # real YouTube API (needs YOUTUBE_API_KEY)

It executes the *same code* that runs in AWS, in the same order as the Step
Functions state machine:

    ingest Lambda -> Glue bronze_to_silver -> DQ (silver) -> Glue silver_to_gold -> DQ (gold)

Storage is a local folder (default ./local_lake) instead of S3, Spark runs in local
mode instead of Glue, and the data quality SQL runs on Spark SQL instead of Athena.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [
    str(ROOT / "src" / "lambdas" / "ingest"),
    str(ROOT / "src" / "lambdas" / "data_quality"),
    str(ROOT / "src" / "glue"),
    str(ROOT / "scripts"),
]

import bronze_to_silver  # noqa: E402
import dq_handler  # noqa: E402
import ingest_handler  # noqa: E402
import silver_to_gold  # noqa: E402
from fake_youtube_api import FakeYouTubeApi  # noqa: E402
from rules import TABLES  # noqa: E402

REGIONS = ingest_handler.DEFAULT_REGIONS.split(",")


class SparkQueryRunner:
    """Local stand-in for Athena: registers the lake folders as Spark views."""

    def __init__(self, spark, lake_root: str):
        self.spark = spark
        locations = {
            "silver": "silver/trending_videos",
            "quarantine": "silver/quarantine_trending_videos",
            "gold_videos": "gold/daily_video_trending",
            "gold_kpis": "gold/region_daily_kpis",
            "gold_categories": "gold/region_category_daily",
            "gold_channels": "gold/channel_daily_leaderboard",
            "gold_cross_region": "gold/cross_region_daily",
        }
        for key, rel in locations.items():
            path = Path(lake_root) / rel
            if path.exists():
                spark.read.parquet(str(path)).createOrReplaceTempView(TABLES[key])

    def run_many(self, queries: dict[str, str]) -> dict[str, float | None]:
        out = {}
        for name, sql in queries.items():
            value = self.spark.sql(sql).collect()[0][0]
            out[name] = None if value is None else float(value)
        return out


def make_spark():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("youtube-trending-local")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def run_quality(spark, lake: str, suite: str, ingest: dict) -> dict:
    event = {
        "suite": suite,
        "trending_date": ingest["ingest_date"],
        "ingest_hour": ingest["ingest_hour"],
        "run_id": ingest["run_id"],
    }
    os.environ.pop("BUCKET", None)  # don't try to write results to S3 locally
    result = dq_handler.lambda_handler(event, runner=SparkQueryRunner(spark, lake))
    print(f"  DQ {suite:<6} -> {result['status']} ({result['checks_total']} checks)")
    for name in result["checks_failed"]:
        print(f"     ! failed: {name}")
    return result


def run_pipeline(lake: Path, start: datetime, runs: int, live: bool, spark) -> list[dict]:
    lake_uri = str(lake)
    storage = ingest_handler.LocalStorage(lake)
    summaries = []
    for i in range(runs):
        at = start + timedelta(hours=6 * i)
        ctx = ingest_handler.build_run_context({"run_id": f"local-{at:%Y%m%dT%H}", "execution_start": at.isoformat()})
        if live:
            api_key = os.environ.get("YOUTUBE_API_KEY") or sys.exit("Set YOUTUBE_API_KEY for --live runs")
            ingest = ingest_handler.ingest(REGIONS, api_key, storage, ctx)
        else:
            ingest = ingest_handler.ingest(REGIONS, "fake-key", storage, ctx, http_get=FakeYouTubeApi(now=at))
        ingest = ingest.to_dict()
        print(
            f"\n[{ingest['run_id']}] ingest: {ingest['video_count']} videos from "
            f"{len(ingest['regions_succeeded'])} regions"
        )

        stats = bronze_to_silver.run(spark, lake_uri, ingest["ingest_date"], ingest["ingest_hour"])
        print(f"  bronze -> silver: {stats}")
        silver_dq = run_quality(spark, lake_uri, "silver", ingest)
        if silver_dq["status"] == "FAILED":
            print("  Silver quality gate FAILED - stopping (Step Functions would alert via SNS).")
            break

        counts = silver_to_gold.run(spark, lake_uri, ingest["ingest_date"])
        print(f"  silver -> gold:   {counts}")
        gold_dq = run_quality(spark, lake_uri, "gold", ingest)
        summaries.append({"ingest": ingest, "silver_dq": silver_dq, "gold_dq": gold_dq})
    return summaries


def show_insights(spark, lake: Path, trending_date: str) -> None:
    gold = lake / "gold"
    print("\n=== Region KPIs (gold_region_daily_kpis) ===")
    spark.read.parquet(str(gold / "region_daily_kpis")).where(f"trending_date = '{trending_date}'").select(
        "region", "unique_videos", "median_views", "avg_engagement_rate", "avg_hours_to_trend", "hd_share"
    ).orderBy("region").show(20, truncate=False)

    print("=== Videos trending in the most regions (gold_cross_region_daily) ===")
    spark.read.parquet(str(gold / "cross_region_daily")).where(f"trending_date = '{trending_date}'").orderBy(
        "regions_count", "max_views", ascending=False
    ).select("title", "regions_count", "regions", "max_views").show(5, truncate=40)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--lake", type=Path, default=ROOT / "local_lake", help="Local data lake folder")
    parser.add_argument("--runs", type=int, default=2, help="Intraday runs to simulate (6h apart)")
    parser.add_argument("--live", action="store_true", help="Call the real YouTube API")
    parser.add_argument("--keep", action="store_true", help="Keep existing lake contents")
    args = parser.parse_args(argv)

    if args.lake.exists() and not args.keep:
        shutil.rmtree(args.lake)
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = now if args.live else now.replace(hour=3)
    runs = 1 if args.live else max(1, min(args.runs, 4))

    spark = make_spark()
    try:
        summaries = run_pipeline(args.lake, start, runs, args.live, spark)
        if summaries:
            show_insights(spark, args.lake, summaries[-1]["ingest"]["ingest_date"])
        print(f"Lake written to: {args.lake}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
