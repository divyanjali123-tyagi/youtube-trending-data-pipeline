"""Glue job logic (run on local Spark): Bronze -> Silver -> Gold."""

from datetime import datetime, timezone

import pytest

import bronze_to_silver as b2s
import ingest_handler as ih
import silver_to_gold as s2g
from fake_youtube_api import FakeYouTubeApi

REGIONS = ["US", "GB", "IN", "JP"]
DATE = "2026-10-25"


@pytest.fixture(scope="module")
def lake(tmp_path_factory, spark):
    root = tmp_path_factory.mktemp("lake")
    for hour in (3, 9):
        now = datetime(2026, 10, 25, hour, tzinfo=timezone.utc)
        c = ih.build_run_context({"run_id": f"run-{hour}", "execution_start": now.isoformat()})
        ih.ingest(REGIONS, "k", ih.LocalStorage(root), c, http_get=FakeYouTubeApi(now=now, videos_per_region=30))
        b2s.run(spark, str(root), DATE, f"{hour:02d}")
    s2g.run(spark, str(root), DATE)
    return str(root)


def test_iso_duration_parsing(spark):
    from pyspark.sql import functions as F

    rows = spark.createDataFrame(
        [("PT45S",), ("PT1H2M3S",), ("P1DT1M",), ("PT10M",), ("P0D",), (None,), ("garbage",)], ["d"]
    ).select(b2s.iso8601_duration_to_seconds(F.col("d")).alias("s"))
    assert [r.s for r in rows.collect()] == [45, 3723, 86460, 600, 0, None, None]


def test_silver_rows_quarantine_and_dedup(spark, lake):
    silver = spark.read.parquet(f"{lake}/silver/trending_videos").where("ingest_hour = '09'")
    quarantine = spark.read.parquet(f"{lake}/silver/quarantine_trending_videos").where("ingest_hour = '09'")

    # 30 videos + 1 duplicate per region; 2 broken records per region are quarantined
    assert silver.count() == len(REGIONS) * 28
    reasons = {r.quarantine_reason: r["count"] for r in quarantine.groupBy("quarantine_reason").count().collect()}
    assert reasons == {"invalid_published_at": len(REGIONS), "missing_view_count": len(REGIONS)}
    assert silver.select("video_id", "region").distinct().count() == silver.count()


def test_silver_typing_and_features(spark, lake):
    row = spark.read.parquet(f"{lake}/silver/trending_videos").where("view_count > 0").first()
    assert row.category_name is not None
    assert row.ingest_hour in {"03", "09"} and row.trending_date == DATE
    expected = round(((row.like_count or 0) + (row.comment_count or 0)) / row.view_count, 6)
    assert row.engagement_rate == pytest.approx(expected)
    assert row.hours_to_trend > 0 and row.tag_count == len(row.tags)


def test_hidden_likes_are_kept_as_null(spark, lake):
    silver = spark.read.parquet(f"{lake}/silver/trending_videos")
    assert silver.where("like_count IS NULL").count() > 0


def test_rerun_is_idempotent(spark, lake):
    before = spark.read.parquet(f"{lake}/silver/trending_videos").count()
    b2s.run(spark, lake, DATE, "09")
    assert spark.read.parquet(f"{lake}/silver/trending_videos").count() == before


def test_gold_daily_videos_collapse_snapshots(spark, lake):
    videos = spark.read.parquet(f"{lake}/gold/daily_video_trending")
    silver = spark.read.parquet(f"{lake}/silver/trending_videos")
    assert videos.count() == silver.select("video_id", "region").distinct().count()
    assert videos.where("snapshots = 2").count() > 0
    assert videos.where("view_gain < 0").count() == 0


def test_gold_kpis_and_category_shares(spark, lake):
    kpis = spark.read.parquet(f"{lake}/gold/region_daily_kpis")
    assert sorted(r.region for r in kpis.collect()) == sorted(REGIONS)
    shares = spark.read.parquet(f"{lake}/gold/region_category_daily").groupBy("region").sum("share_of_videos")
    for r in shares.collect():
        assert r[1] == pytest.approx(1.0, abs=1e-3)


def test_gold_cross_region_and_leaderboard(spark, lake):
    cross = spark.read.parquet(f"{lake}/gold/cross_region_daily")
    assert cross.where("regions_count > 1").count() > 0
    assert all(r.regions_count == len(r.regions) for r in cross.collect())
    board = spark.read.parquet(f"{lake}/gold/channel_daily_leaderboard")
    assert board.groupBy("region", "leaderboard_rank").count().where("count > 1").count() == 0
