"""Ingest Lambda: pagination, retries, quota handling, Bronze layout."""

import io
import json
import urllib.error
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

import ingest_handler as ih
from fake_youtube_api import FakeYouTubeApi

NOW = datetime(2026, 10, 25, 9, 0, tzinfo=timezone.utc)


def ctx():
    return ih.build_run_context({"run_id": "exec-1", "execution_start": "2026-10-25T09:00:00.123Z"})


def http_error(code, reason=""):
    body = json.dumps({"error": {"errors": [{"reason": reason}]}}).encode()
    return urllib.error.HTTPError("https://x", code, "err", {}, io.BytesIO(body))


def test_run_context_from_step_functions_input():
    c = ctx()
    assert (c.run_id, c.ingest_date, c.ingest_hour) == ("exec-1", "2026-10-25", "09")
    assert c.ingested_at_iso == "2026-10-25T09:00:00Z"


def test_run_id_is_sanitised():
    c = ih.build_run_context({"run_id": "a b/c:d"}, now=NOW)
    assert c.run_id == "a-b-c-d"


def test_fetch_trending_paginates_and_caps():
    api = FakeYouTubeApi(now=NOW, videos_per_region=120, inject_errors=False)
    items = ih.fetch_trending(api, "k", "US", max_results=110)
    assert len(items) == 110
    assert sum("chart=mostPopular" in u for u in api.calls) == 3  # 50 + 50 + 10


def test_ingest_writes_bronze_jsonl_with_metadata(tmp_path):
    api = FakeYouTubeApi(now=NOW, videos_per_region=20)
    result = ih.ingest(["US", "IN"], "k", ih.LocalStorage(tmp_path), ctx(), http_get=api)

    assert result.regions_succeeded == ["US", "IN"]
    path = tmp_path / "bronze/youtube/trending_videos/region=US/ingest_date=2026-10-25/ingest_hour=09/exec-1.jsonl"
    lines = [json.loads(line) for line in path.read_text().splitlines()]
    assert lines[0]["ingestion"]["trending_rank"] == 1
    assert lines[0]["ingestion"]["region"] == "US"
    assert "snippet" in lines[0]["payload"]  # raw payload kept untouched
    assert (
        tmp_path / "bronze/youtube/video_categories/region=IN/ingest_date=2026-10-25/ingest_hour=09/exec-1.jsonl"
    ).exists()


def test_retry_on_transient_errors_then_succeed():
    calls = {"n": 0}

    def flaky(url):
        calls["n"] += 1
        if calls["n"] < 3:
            raise http_error(503)
        return {"items": []}

    sleeps = []
    ih.call_api(flaky, "videos", {}, "k", sleep=sleeps.append)
    assert calls["n"] == 3
    assert sleeps == [1.0, 2.0]


def test_quota_exceeded_aborts_whole_run(tmp_path):
    def quota(url):
        raise http_error(403, "quotaExceeded")

    with pytest.raises(ih.QuotaExceededError):
        ih.ingest(["US", "GB"], "k", ih.LocalStorage(tmp_path), ctx(), http_get=quota)


def test_region_failure_is_isolated(tmp_path):
    api = FakeYouTubeApi(now=NOW, videos_per_region=10)

    def partially_broken(url):
        if "regionCode=GB" in url:
            raise http_error(400, "invalidRegionCode")
        return api(url)

    result = ih.ingest(
        ["US", "GB"], "k", ih.LocalStorage(tmp_path), ctx(), http_get=partially_broken, sleep=lambda s: None
    )
    assert result.regions_succeeded == ["US"]
    assert "GB" in result.regions_failed


@mock_aws
def test_lambda_handler_writes_to_s3(monkeypatch):
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="lake")
    boto3.client("ssm", region_name="us-east-1").put_parameter(
        Name="/youtube-trending/api-key", Value="secret", Type="SecureString"
    )
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("DATA_LAKE_URI", "s3://lake")
    monkeypatch.setenv("REGIONS", "US,JP")
    monkeypatch.setenv("API_KEY_PARAMETER", "/youtube-trending/api-key")
    monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)

    seen_keys = []
    api = FakeYouTubeApi(now=NOW, videos_per_region=10)

    def recording(url):
        seen_keys.append("key=secret" in url)
        return api(url)

    monkeypatch.setattr(ih, "http_get_json", recording)
    monkeypatch.setattr(ih.ingest, "__defaults__", (200, recording))

    out = ih.lambda_handler({"run_id": "exec-9", "execution_start": "2026-10-25T09:00:00Z"})
    assert out["regions_succeeded"] == ["US", "JP"]
    assert all(seen_keys)
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="lake")["Contents"]]
    assert len(keys) == 4 and all(k.startswith("bronze/youtube/") for k in keys)


def test_lambda_handler_fails_when_every_region_fails(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_LAKE_URI", str(tmp_path))
    monkeypatch.setenv("REGIONS", "US")
    monkeypatch.setenv("YOUTUBE_API_KEY", "k")

    def broken(url):
        raise http_error(400, "badRequest")

    monkeypatch.setattr(ih.ingest, "__defaults__", (200, broken))
    with pytest.raises(ih.ApiError):
        ih.lambda_handler({})
