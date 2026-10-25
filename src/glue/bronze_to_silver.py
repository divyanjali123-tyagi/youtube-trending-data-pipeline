"""AWS Glue job: Bronze (raw JSON Lines) -> Silver (clean, typed Parquet).

Processes exactly one ingestion run (``INGEST_DATE`` + ``INGEST_HOUR``) across all
regions:

* applies an explicit schema to the raw API payloads,
* flattens and type-casts fields (string counters -> bigint, ISO-8601 -> timestamp,
  ``PT1H2M3S`` durations -> seconds),
* enriches each video with its region-specific category name,
* derives analytics features (engagement rate, hours-to-trend, tag count, ...),
* de-duplicates on (video_id, region) keeping the best trending rank,
* routes records that break row-level rules to a quarantine table with a reason.

Output partitions: trending_date / ingest_hour / region (dynamic overwrite, so
re-running a run is idempotent).

The transformation functions are pure PySpark and are unit-tested locally; only
``main()`` touches the Glue runtime.
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

VALID_REGIONS = ["US", "GB", "IN", "CA", "AU", "DE", "FR", "JP", "KR", "BR"]

INGESTION_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType()),
        T.StructField("region", T.StringType()),
        T.StructField("ingested_at", T.StringType()),
        T.StructField("source", T.StringType()),
        T.StructField("endpoint", T.StringType()),
        T.StructField("trending_rank", T.IntegerType()),
    ]
)

VIDEO_SCHEMA = T.StructType(
    [
        T.StructField("ingestion", INGESTION_SCHEMA),
        T.StructField(
            "payload",
            T.StructType(
                [
                    T.StructField("id", T.StringType()),
                    T.StructField(
                        "snippet",
                        T.StructType(
                            [
                                T.StructField("publishedAt", T.StringType()),
                                T.StructField("channelId", T.StringType()),
                                T.StructField("title", T.StringType()),
                                T.StructField("channelTitle", T.StringType()),
                                T.StructField("tags", T.ArrayType(T.StringType())),
                                T.StructField("categoryId", T.StringType()),
                                T.StructField("defaultAudioLanguage", T.StringType()),
                                T.StructField("liveBroadcastContent", T.StringType()),
                            ]
                        ),
                    ),
                    T.StructField(
                        "contentDetails",
                        T.StructType(
                            [
                                T.StructField("duration", T.StringType()),
                                T.StructField("definition", T.StringType()),
                                T.StructField("caption", T.StringType()),
                                T.StructField("licensedContent", T.BooleanType()),
                            ]
                        ),
                    ),
                    T.StructField(
                        "statistics",
                        T.StructType(
                            [
                                T.StructField("viewCount", T.StringType()),
                                T.StructField("likeCount", T.StringType()),
                                T.StructField("commentCount", T.StringType()),
                            ]
                        ),
                    ),
                ]
            ),
        ),
    ]
)

CATEGORY_SCHEMA = T.StructType(
    [
        T.StructField("ingestion", INGESTION_SCHEMA),
        T.StructField(
            "payload",
            T.StructType(
                [
                    T.StructField("id", T.StringType()),
                    T.StructField("snippet", T.StructType([T.StructField("title", T.StringType())])),
                ]
            ),
        ),
    ]
)

SILVER_COLUMNS = [
    "video_id",
    "title",
    "channel_id",
    "channel_title",
    "category_id",
    "category_name",
    "published_at",
    "tags",
    "tag_count",
    "title_length",
    "duration_seconds",
    "definition",
    "has_captions",
    "licensed_content",
    "default_audio_language",
    "view_count",
    "like_count",
    "comment_count",
    "engagement_rate",
    "trending_rank",
    "hours_to_trend",
    "ingested_at",
    "run_id",
    # partition columns (last, in partition order)
    "trending_date",
    "ingest_hour",
    "region",
]

QUARANTINE_COLUMNS = [
    "video_id",
    "region",
    "run_id",
    "ingested_at",
    "quarantine_reason",
    "raw_record",
    "trending_date",
    "ingest_hour",
]

SPARK_CONF = {
    # Keep partition values as strings ("09", "2026-10-25") to match the Glue catalog.
    "spark.sql.sources.partitionColumnTypeInference.enabled": "false",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.session.timeZone": "UTC",
    # Malformed timestamps/numbers become NULL (and are quarantined) instead of failing the job.
    "spark.sql.ansi.enabled": "false",
}


# ------------------------------------------------------------------ paths / IO


def bronze_glob(lake_root: str, dataset: str, ingest_date: str, ingest_hour: str) -> str:
    return f"{lake_root}/bronze/youtube/{dataset}/region=*/ingest_date={ingest_date}/ingest_hour={ingest_hour}/*.jsonl"


def read_bronze(spark: SparkSession, lake_root: str, ingest_date: str, ingest_hour: str):
    videos = spark.read.schema(VIDEO_SCHEMA).json(bronze_glob(lake_root, "trending_videos", ingest_date, ingest_hour))
    categories = spark.read.schema(CATEGORY_SCHEMA).json(
        bronze_glob(lake_root, "video_categories", ingest_date, ingest_hour)
    )
    return videos, categories


# -------------------------------------------------------------- transformations


def iso8601_duration_to_seconds(col: F.Column) -> F.Column:
    """Convert ISO-8601 durations such as P1DT2H3M4S / PT45S to seconds (null if absent)."""
    pattern = r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$"

    def part(idx: int) -> F.Column:
        raw = F.regexp_extract(col, pattern, idx)
        return F.coalesce(F.when(raw != "", raw.cast("int")), F.lit(0))

    matches = col.rlike(pattern)
    seconds = part(1) * 86400 + part(2) * 3600 + part(3) * 60 + part(4)
    return F.when(col.isNotNull() & matches, seconds).cast("int")


def parse_utc_timestamp(col: F.Column) -> F.Column:
    return F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss'Z'")


def flatten_videos(videos: DataFrame) -> DataFrame:
    p, i = "payload", "ingestion"
    tags = F.coalesce(F.col(f"{p}.snippet.tags"), F.array().cast(T.ArrayType(T.StringType())))
    ingested_at = parse_utc_timestamp(F.col(f"{i}.ingested_at"))
    return videos.select(
        F.trim(F.col(f"{p}.id")).alias("video_id"),
        F.trim(F.col(f"{p}.snippet.title")).alias("title"),
        F.col(f"{p}.snippet.channelId").alias("channel_id"),
        F.trim(F.col(f"{p}.snippet.channelTitle")).alias("channel_title"),
        F.col(f"{p}.snippet.categoryId").alias("category_id"),
        F.col(f"{p}.snippet.publishedAt").alias("published_at_raw"),
        parse_utc_timestamp(F.col(f"{p}.snippet.publishedAt")).alias("published_at"),
        tags.alias("tags"),
        F.size(tags).cast("int").alias("tag_count"),
        F.length(F.col(f"{p}.snippet.title")).cast("int").alias("title_length"),
        iso8601_duration_to_seconds(F.col(f"{p}.contentDetails.duration")).alias("duration_seconds"),
        F.lower(F.col(f"{p}.contentDetails.definition")).alias("definition"),
        (F.lower(F.col(f"{p}.contentDetails.caption")) == "true").alias("has_captions"),
        F.col(f"{p}.contentDetails.licensedContent").alias("licensed_content"),
        F.col(f"{p}.snippet.defaultAudioLanguage").alias("default_audio_language"),
        F.col(f"{p}.statistics.viewCount").cast("bigint").alias("view_count"),
        F.col(f"{p}.statistics.likeCount").cast("bigint").alias("like_count"),
        F.col(f"{p}.statistics.commentCount").cast("bigint").alias("comment_count"),
        F.col(f"{i}.trending_rank").alias("trending_rank"),
        ingested_at.alias("ingested_at"),
        F.col(f"{i}.run_id").alias("run_id"),
        F.upper(F.col(f"{i}.region")).alias("region"),
        F.date_format(ingested_at, "yyyy-MM-dd").alias("trending_date"),
        F.date_format(ingested_at, "HH").alias("ingest_hour"),
        F.to_json(F.col(p)).alias("raw_record"),
    )


def category_lookup(categories: DataFrame) -> DataFrame:
    return (
        categories.select(
            F.upper(F.col("ingestion.region")).alias("region"),
            F.col("payload.id").alias("category_id"),
            F.col("payload.snippet.title").alias("category_name"),
        )
        .where(F.col("category_id").isNotNull())
        .dropDuplicates(["region", "category_id"])
    )


def quarantine_reason() -> F.Column:
    """First failing row-level rule, or null when the record is valid."""
    return (
        F.when(F.col("video_id").isNull() | (F.col("video_id") == ""), F.lit("missing_video_id"))
        .when(~F.col("region").isin(VALID_REGIONS) | F.col("region").isNull(), F.lit("unknown_region"))
        .when(F.col("published_at").isNull(), F.lit("invalid_published_at"))
        .when(F.col("view_count").isNull(), F.lit("missing_view_count"))
        .when(
            (F.col("view_count") < 0)
            | (F.coalesce(F.col("like_count"), F.lit(0)) < 0)
            | (F.coalesce(F.col("comment_count"), F.lit(0)) < 0),
            F.lit("negative_metric"),
        )
        .when(F.col("trending_rank").isNull() | (F.col("trending_rank") < 1), F.lit("invalid_rank"))
    )


def add_features(df: DataFrame) -> DataFrame:
    interactions = F.coalesce(F.col("like_count"), F.lit(0)) + F.coalesce(F.col("comment_count"), F.lit(0))
    return df.withColumn(
        "engagement_rate",
        F.when(F.col("view_count") > 0, F.round(interactions / F.col("view_count"), 6)).cast("double"),
    ).withColumn(
        "hours_to_trend",
        F.round((F.col("ingested_at").cast("long") - F.col("published_at").cast("long")) / 3600.0, 2).cast("double"),
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """One row per (video_id, region) per run; keep the best (lowest) trending rank."""
    w = Window.partitionBy("video_id", "region", "run_id").orderBy(F.col("trending_rank").asc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def transform(videos: DataFrame, categories: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (silver_df, quarantine_df)."""
    flat = flatten_videos(videos).withColumn("quarantine_reason", quarantine_reason())

    quarantine = flat.where(F.col("quarantine_reason").isNotNull()).select(*QUARANTINE_COLUMNS)

    valid = flat.where(F.col("quarantine_reason").isNull())
    valid = deduplicate(valid)
    valid = valid.join(F.broadcast(category_lookup(categories)), ["region", "category_id"], "left")
    silver = add_features(valid).select(*SILVER_COLUMNS)
    return silver, quarantine


def write_outputs(silver: DataFrame, quarantine: DataFrame, lake_root: str) -> None:
    (
        silver.repartition("region")
        .write.mode("overwrite")
        .partitionBy("trending_date", "ingest_hour", "region")
        .parquet(f"{lake_root}/silver/trending_videos")
    )
    (
        quarantine.coalesce(1)
        .write.mode("overwrite")
        .partitionBy("trending_date", "ingest_hour")
        .parquet(f"{lake_root}/silver/quarantine_trending_videos")
    )


def run(spark: SparkSession, lake_root: str, ingest_date: str, ingest_hour: str) -> dict:
    for key, value in SPARK_CONF.items():
        spark.conf.set(key, value)
    videos, categories = read_bronze(spark, lake_root, ingest_date, ingest_hour)
    silver, quarantine = transform(videos, categories)
    silver.cache()
    quarantine.cache()
    write_outputs(silver, quarantine, lake_root)
    stats = {"silver_rows": silver.count(), "quarantined_rows": quarantine.count()}
    silver.unpersist()
    quarantine.unpersist()
    return stats


# ------------------------------------------------------------------------ Glue


def main() -> None:  # pragma: no cover - requires the Glue runtime
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "INGEST_DATE", "INGEST_HOUR"])
    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    stats = run(glue_context.spark_session, f"s3://{args['BUCKET']}", args["INGEST_DATE"], args["INGEST_HOUR"])
    print(f"Bronze -> Silver complete: {stats}")
    job.commit()


if __name__ == "__main__":  # pragma: no cover
    main()
