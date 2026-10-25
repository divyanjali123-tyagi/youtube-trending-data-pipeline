"""Ingest Lambda: YouTube Data API v3 (mostPopular chart) -> Bronze layer.

For every configured region it pulls the most-popular ("trending") video chart and
the region's video-category lookup, then writes the *raw, untouched* API items to
the Bronze zone as JSON Lines, wrapped with ingestion metadata:

    bronze/youtube/trending_videos/region=US/ingest_date=2026-10-25/ingest_hour=09/<run_id>.jsonl
    bronze/youtube/video_categories/region=US/ingest_date=2026-10-25/ingest_hour=09/<run_id>.jsonl

Each line: {"ingestion": {...metadata...}, "payload": {...raw API item...}}

The handler is invoked by Step Functions and returns the run coordinates
(run_id, ingest_date, ingest_hour) that downstream Glue jobs use to process
exactly this run's partitions.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

logger = logging.getLogger()
logger.setLevel(logging.INFO)

API_BASE = "https://www.googleapis.com/youtube/v3"
DEFAULT_REGIONS = "US,GB,IN,CA,AU,DE,FR,JP,KR,BR"
VIDEO_PARTS = "snippet,contentDetails,statistics"
PAGE_SIZE = 50  # API maximum per page
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

TRENDING_DATASET = "youtube/trending_videos"
CATEGORIES_DATASET = "youtube/video_categories"


class QuotaExceededError(RuntimeError):
    """The daily YouTube API quota is exhausted - retrying is pointless."""


class ApiError(RuntimeError):
    pass


# --------------------------------------------------------------------------- HTTP


HttpGet = Callable[[str], dict]


def http_get_json(url: str, timeout: int = 20) -> dict:
    """GET a URL and decode JSON. Raises urllib.error.HTTPError on non-2xx."""
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - fixed https host
        return json.loads(resp.read().decode("utf-8"))


def _error_reason(err: urllib.error.HTTPError) -> str:
    try:
        body = json.loads(err.read().decode("utf-8"))
        errors = body.get("error", {}).get("errors", [])
        return errors[0].get("reason", "") if errors else body.get("error", {}).get("message", "")
    except Exception:  # noqa: BLE001 - best effort parsing of error body
        return ""


def call_api(
    http_get: HttpGet,
    endpoint: str,
    params: dict,
    api_key: str,
    max_attempts: int = 4,
    base_delay: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> dict:
    """Call a YouTube API endpoint with exponential backoff on transient errors."""
    query = urllib.parse.urlencode({**params, "key": api_key})
    url = f"{API_BASE}/{endpoint}?{query}"
    for attempt in range(1, max_attempts + 1):
        try:
            return http_get(url)
        except urllib.error.HTTPError as err:
            reason = _error_reason(err)
            if err.code == 403 and reason in {"quotaExceeded", "dailyLimitExceeded"}:
                raise QuotaExceededError("YouTube API daily quota exceeded") from err
            if err.code not in RETRYABLE_STATUS or attempt == max_attempts:
                raise ApiError(f"{endpoint} failed with HTTP {err.code} {reason}".strip()) from err
        except (urllib.error.URLError, TimeoutError) as err:
            if attempt == max_attempts:
                raise ApiError(f"{endpoint} network error: {err}") from err
        delay = base_delay * 2 ** (attempt - 1)
        logger.warning("Retrying %s (attempt %d/%d) in %.1fs", endpoint, attempt, max_attempts, delay)
        sleep(delay)
    raise ApiError(f"{endpoint} failed")  # pragma: no cover


def fetch_trending(http_get: HttpGet, api_key: str, region: str, max_results: int, **kw) -> list[dict]:
    """Page through the mostPopular chart for one region (API caps it at 200 videos)."""
    items: list[dict] = []
    page_token: str | None = None
    while len(items) < max_results:
        params = {
            "part": VIDEO_PARTS,
            "chart": "mostPopular",
            "regionCode": region,
            "maxResults": min(PAGE_SIZE, max_results - len(items)),
        }
        if page_token:
            params["pageToken"] = page_token
        body = call_api(http_get, "videos", params, api_key, **kw)
        items.extend(body.get("items", []))
        page_token = body.get("nextPageToken")
        if not page_token:
            break
    return items[:max_results]


def fetch_categories(http_get: HttpGet, api_key: str, region: str, **kw) -> list[dict]:
    params = {"part": "snippet", "regionCode": region, "hl": "en_US"}
    return call_api(http_get, "videoCategories", params, api_key, **kw).get("items", [])


# ------------------------------------------------------------------------ Storage


class Storage(Protocol):
    def put(self, key: str, body: bytes) -> str: ...


@dataclass
class S3Storage:
    bucket: str
    client: object = None

    def put(self, key: str, body: bytes) -> str:
        if self.client is None:
            import boto3

            self.client = boto3.client("s3")
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/x-ndjson")
        return f"s3://{self.bucket}/{key}"


@dataclass
class LocalStorage:
    root: Path

    def put(self, key: str, body: bytes) -> str:
        path = Path(self.root) / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return str(path)


def storage_from_uri(uri: str) -> Storage:
    """``s3://bucket`` -> S3Storage, anything else -> local directory."""
    if uri.startswith("s3://"):
        return S3Storage(bucket=uri[5:].split("/", 1)[0])
    return LocalStorage(Path(uri))


# ------------------------------------------------------------------------- Run


@dataclass
class RunContext:
    run_id: str
    ingested_at: datetime

    @property
    def ingest_date(self) -> str:
        return self.ingested_at.strftime("%Y-%m-%d")

    @property
    def ingest_hour(self) -> str:
        return self.ingested_at.strftime("%H")

    @property
    def ingested_at_iso(self) -> str:
        return self.ingested_at.strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class IngestResult:
    run_id: str
    ingest_date: str
    ingest_hour: str
    regions_succeeded: list[str] = field(default_factory=list)
    regions_failed: dict[str, str] = field(default_factory=dict)
    video_count: int = 0
    category_count: int = 0
    objects: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "ingest_date": self.ingest_date,
            "ingest_hour": self.ingest_hour,
            "regions_succeeded": self.regions_succeeded,
            "regions_failed": self.regions_failed,
            "video_count": self.video_count,
            "category_count": self.category_count,
            "objects_written": len(self.objects),
        }


def _safe_run_id(raw: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "-", raw)[:80]


def build_run_context(event: dict, now: datetime | None = None) -> RunContext:
    """Derive the run id and ingestion timestamp from the Step Functions input."""
    now = now or datetime.now(timezone.utc)
    started = event.get("execution_start")
    if started:
        # Step Functions supplies e.g. 2026-10-25T09:00:00.123Z
        ingested_at = datetime.fromisoformat(started.replace("Z", "+00:00")).astimezone(timezone.utc)
    else:
        ingested_at = now
    run_id = _safe_run_id(event.get("run_id") or ingested_at.strftime("manual-%Y%m%dT%H%M%S"))
    return RunContext(run_id=run_id, ingested_at=ingested_at.replace(microsecond=0))


def _key(dataset: str, region: str, ctx: RunContext) -> str:
    return (
        f"bronze/{dataset}/region={region}/ingest_date={ctx.ingest_date}/"
        f"ingest_hour={ctx.ingest_hour}/{ctx.run_id}.jsonl"
    )


def _jsonl(lines: list[dict]) -> bytes:
    return ("\n".join(json.dumps(line, ensure_ascii=False) for line in lines) + "\n").encode("utf-8")


def ingest(
    regions: list[str],
    api_key: str,
    storage: Storage,
    ctx: RunContext,
    max_results: int = 200,
    http_get: HttpGet = http_get_json,
    **api_kwargs,
) -> IngestResult:
    result = IngestResult(run_id=ctx.run_id, ingest_date=ctx.ingest_date, ingest_hour=ctx.ingest_hour)

    for region in regions:
        try:
            videos = fetch_trending(http_get, api_key, region, max_results, **api_kwargs)
            categories = fetch_categories(http_get, api_key, region, **api_kwargs)
        except QuotaExceededError:
            raise
        except ApiError as err:
            logger.error("Region %s failed: %s", region, err)
            result.regions_failed[region] = str(err)
            continue

        base_meta = {
            "run_id": ctx.run_id,
            "region": region,
            "ingested_at": ctx.ingested_at_iso,
            "source": "youtube.data.v3",
        }
        video_lines = [
            {"ingestion": {**base_meta, "endpoint": "videos.list", "trending_rank": rank}, "payload": item}
            for rank, item in enumerate(videos, start=1)
        ]
        category_lines = [
            {"ingestion": {**base_meta, "endpoint": "videoCategories.list"}, "payload": item} for item in categories
        ]
        if video_lines:
            result.objects.append(storage.put(_key(TRENDING_DATASET, region, ctx), _jsonl(video_lines)))
        if category_lines:
            result.objects.append(storage.put(_key(CATEGORIES_DATASET, region, ctx), _jsonl(category_lines)))

        result.regions_succeeded.append(region)
        result.video_count += len(video_lines)
        result.category_count += len(category_lines)
        logger.info("Region %s: %d videos, %d categories", region, len(video_lines), len(category_lines))

    return result


# ---------------------------------------------------------------------- Lambda


def _get_api_key() -> str:
    if os.getenv("YOUTUBE_API_KEY"):
        return os.environ["YOUTUBE_API_KEY"]
    param = os.environ["API_KEY_PARAMETER"]
    import boto3

    ssm = boto3.client("ssm")
    return ssm.get_parameter(Name=param, WithDecryption=True)["Parameter"]["Value"]


def lambda_handler(event: dict, context: object = None) -> dict:
    event = event or {}
    regions = [r.strip().upper() for r in os.getenv("REGIONS", DEFAULT_REGIONS).split(",") if r.strip()]
    max_results = int(os.getenv("MAX_RESULTS_PER_REGION", "200"))
    storage = storage_from_uri(os.environ["DATA_LAKE_URI"])
    ctx = build_run_context(event)

    result = ingest(regions, _get_api_key(), storage, ctx, max_results=max_results)
    logger.info("Ingest summary: %s", json.dumps(result.to_dict()))

    if not result.regions_succeeded:
        raise ApiError(f"All regions failed: {result.regions_failed}")
    return result.to_dict()
