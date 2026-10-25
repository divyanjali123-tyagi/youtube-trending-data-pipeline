"""AWS Glue job: Silver -> Gold (business-level aggregates for Athena).

Recomputes every Gold table for one ``TRENDING_DATE`` from all Silver snapshots of
that day (all ingestion hours, all regions) and overwrites only that date's
partition, so the job is idempotent and can be re-run safely.

Gold tables
-----------
gold_daily_video_trending     one row per (region, video) per day: rank, snapshots, view gain
gold_region_daily_kpis        one row per region per day: volume, medians, engagement, HD share
gold_region_category_daily    category mix per region per day
gold_channel_daily_leaderboard  channels ranked by trending views per region per day
gold_cross_region_daily       videos trending in several regions on the same day
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

SPARK_CONF = {
    "spark.sql.sources.partitionColumnTypeInference.enabled": "false",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.session.timeZone": "UTC",
}

GOLD_TABLES = [
    "daily_video_trending",
    "region_daily_kpis",
    "region_category_daily",
    "channel_daily_leaderboard",
    "cross_region_daily",
]


def read_silver(spark: SparkSession, lake_root: str, trending_date: str) -> DataFrame:
    return (
        spark.read.option("basePath", f"{lake_root}/silver/trending_videos")
        .parquet(f"{lake_root}/silver/trending_videos/trending_date={trending_date}")
        .where(F.col("trending_date") == trending_date)
    )


# ------------------------------------------------------------------ aggregates


def daily_video_trending(silver: DataFrame) -> DataFrame:
    """Collapse intraday snapshots into one row per region/video/day."""
    key = ["trending_date", "region", "video_id"]
    latest_w = Window.partitionBy(*key).orderBy(F.col("ingested_at").desc())
    first_w = Window.partitionBy(*key).orderBy(F.col("ingested_at").asc())

    ranked = silver.withColumn("_latest", F.row_number().over(latest_w)).withColumn(
        "_first", F.row_number().over(first_w)
    )
    agg = ranked.groupBy(*key).agg(
        F.min("trending_rank").cast("int").alias("best_rank"),
        F.count(F.lit(1)).cast("bigint").alias("snapshots"),
        F.min("ingested_at").alias("first_seen_at"),
        F.max("ingested_at").alias("last_seen_at"),
        F.max(F.when(F.col("_first") == 1, F.col("view_count"))).alias("_first_views"),
        F.max(F.when(F.col("_first") == 1, F.col("hours_to_trend"))).alias("hours_to_trend"),
    )
    latest = ranked.where("_latest = 1").select(
        *key,
        "title",
        "channel_id",
        "channel_title",
        "category_name",
        "published_at",
        "duration_seconds",
        F.col("trending_rank").alias("latest_rank"),
        F.col("view_count").alias("latest_view_count"),
        F.col("like_count").alias("latest_like_count"),
        F.col("comment_count").alias("latest_comment_count"),
        "engagement_rate",
    )
    return latest.join(agg, key).select(
        "region",
        "video_id",
        "title",
        "channel_id",
        "channel_title",
        "category_name",
        "published_at",
        "duration_seconds",
        "best_rank",
        F.col("latest_rank").cast("int"),
        "snapshots",
        "first_seen_at",
        "last_seen_at",
        "latest_view_count",
        "latest_like_count",
        "latest_comment_count",
        (F.col("latest_view_count") - F.col("_first_views")).cast("bigint").alias("view_gain"),
        "engagement_rate",
        "hours_to_trend",
        "trending_date",
    )


def region_daily_kpis(silver: DataFrame, videos: DataFrame) -> DataFrame:
    snapshot_counts = silver.groupBy("trending_date", "region").agg(
        F.count(F.lit(1)).cast("bigint").alias("total_snapshots")
    )
    kpis = videos.groupBy("trending_date", "region").agg(
        F.count(F.lit(1)).cast("bigint").alias("unique_videos"),
        F.round(F.avg("latest_view_count"), 2).cast("double").alias("avg_views"),
        F.percentile_approx(F.col("latest_view_count").cast("double"), 0.5).cast("double").alias("median_views"),
        F.round(F.avg("engagement_rate"), 6).cast("double").alias("avg_engagement_rate"),
        F.round(F.avg("hours_to_trend"), 2).cast("double").alias("avg_hours_to_trend"),
        F.round(F.avg("duration_seconds"), 1).cast("double").alias("avg_duration_seconds"),
    )
    shares = (
        silver.dropDuplicates(["trending_date", "region", "video_id"])
        .groupBy("trending_date", "region")
        .agg(
            F.round(F.avg(F.when(F.col("definition") == "hd", 1.0).otherwise(0.0)), 4).cast("double").alias("hd_share"),
            F.round(F.avg(F.when(F.col("has_captions"), 1.0).otherwise(0.0)), 4).cast("double").alias("captions_share"),
        )
    )
    return (
        kpis.join(snapshot_counts, ["trending_date", "region"])
        .join(shares, ["trending_date", "region"])
        .select(
            "region",
            "unique_videos",
            "total_snapshots",
            "avg_views",
            "median_views",
            "avg_engagement_rate",
            "avg_hours_to_trend",
            "avg_duration_seconds",
            "hd_share",
            "captions_share",
            "trending_date",
        )
    )


def region_category_daily(videos: DataFrame) -> DataFrame:
    category = F.coalesce(F.col("category_name"), F.lit("Unknown"))
    region_w = Window.partitionBy("trending_date", "region")
    return (
        videos.withColumn("category_name", category)
        .groupBy("trending_date", "region", "category_name")
        .agg(
            F.count(F.lit(1)).cast("bigint").alias("video_count"),
            F.sum("latest_view_count").cast("bigint").alias("total_views"),
            F.round(F.avg("latest_view_count"), 2).cast("double").alias("avg_views"),
            F.round(F.avg("engagement_rate"), 6).cast("double").alias("avg_engagement_rate"),
        )
        .withColumn(
            "share_of_videos",
            F.round(F.col("video_count") / F.sum("video_count").over(region_w), 6).cast("double"),
        )
        .select(
            "region",
            "category_name",
            "video_count",
            "total_views",
            "avg_views",
            "avg_engagement_rate",
            "share_of_videos",
            "trending_date",
        )
    )


def channel_daily_leaderboard(videos: DataFrame) -> DataFrame:
    w = Window.partitionBy("trending_date", "region").orderBy(F.col("total_views").desc(), F.col("channel_id"))
    return (
        videos.groupBy("trending_date", "region", "channel_id")
        .agg(
            F.max("channel_title").alias("channel_title"),
            F.count(F.lit(1)).cast("bigint").alias("videos_trending"),
            F.sum("latest_view_count").cast("bigint").alias("total_views"),
            F.min("best_rank").cast("int").alias("best_rank"),
        )
        .withColumn("leaderboard_rank", F.row_number().over(w).cast("int"))
        .select(
            "region",
            "channel_id",
            "channel_title",
            "videos_trending",
            "total_views",
            "best_rank",
            "leaderboard_rank",
            "trending_date",
        )
    )


def cross_region_daily(videos: DataFrame) -> DataFrame:
    return (
        videos.groupBy("trending_date", "video_id")
        .agg(
            F.max("title").alias("title"),
            F.max("channel_title").alias("channel_title"),
            F.max("category_name").alias("category_name"),
            F.countDistinct("region").cast("int").alias("regions_count"),
            F.array_sort(F.collect_set("region")).alias("regions"),
            F.max("latest_view_count").cast("bigint").alias("max_views"),
            F.min("best_rank").cast("int").alias("best_rank"),
        )
        .select(
            "video_id",
            "title",
            "channel_title",
            "category_name",
            "regions_count",
            "regions",
            "max_views",
            "best_rank",
            "trending_date",
        )
    )


def build_gold(silver: DataFrame) -> dict[str, DataFrame]:
    videos = daily_video_trending(silver).cache()
    return {
        "daily_video_trending": videos,
        "region_daily_kpis": region_daily_kpis(silver, videos),
        "region_category_daily": region_category_daily(videos),
        "channel_daily_leaderboard": channel_daily_leaderboard(videos),
        "cross_region_daily": cross_region_daily(videos),
    }


def write_gold(tables: dict[str, DataFrame], lake_root: str) -> dict[str, int]:
    counts = {}
    for name, df in tables.items():
        df.coalesce(1).write.mode("overwrite").partitionBy("trending_date").parquet(f"{lake_root}/gold/{name}")
        counts[name] = df.count()
    return counts


def run(spark: SparkSession, lake_root: str, trending_date: str) -> dict[str, int]:
    for key, value in SPARK_CONF.items():
        spark.conf.set(key, value)
    silver = read_silver(spark, lake_root, trending_date).cache()
    counts = write_gold(build_gold(silver), lake_root)
    silver.unpersist()
    return counts


def main() -> None:  # pragma: no cover - requires the Glue runtime
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "TRENDING_DATE"])
    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    counts = run(glue_context.spark_session, f"s3://{args['BUCKET']}", args["TRENDING_DATE"])
    print(f"Silver -> Gold complete: {counts}")
    job.commit()


if __name__ == "__main__":  # pragma: no cover
    main()
