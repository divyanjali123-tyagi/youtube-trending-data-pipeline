"""Deterministic fake of the YouTube Data API v3 for offline runs and tests.

It returns responses with the same shape as ``videos.list(chart=mostPopular)`` and
``videoCategories.list`` so the real ingest Lambda code runs unchanged. All data is
synthetic. A handful of deliberately broken records are injected so the Silver
quality rules (quarantine, de-duplication) have something to catch.
"""

from __future__ import annotations

import hashlib
import json
import random
import urllib.parse
import zlib
from datetime import datetime, timedelta, timezone

CATEGORIES = {
    "1": "Film & Animation",
    "2": "Autos & Vehicles",
    "10": "Music",
    "17": "Sports",
    "20": "Gaming",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "27": "Education",
    "28": "Science & Technology",
}

CHANNELS = [f"UC{hashlib.md5(str(i).encode()).hexdigest()[:22]}" for i in range(60)]
GLOBAL_POOL = 40  # videos that can trend in several regions at once


def _video_id(n: int) -> str:
    return hashlib.sha1(f"video-{n}".encode()).hexdigest()[:11]


def _duration(seconds: int) -> str:
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    out = "PT"
    if h:
        out += f"{h}H"
    if m:
        out += f"{m}M"
    if s or out == "PT":
        out += f"{s}S"
    return out


class FakeYouTubeApi:
    """Callable compatible with the ingest Lambda's ``http_get(url) -> dict``."""

    def __init__(self, now: datetime | None = None, videos_per_region: int = 50, inject_errors: bool = True):
        self.now = now or datetime.now(timezone.utc)
        self.videos_per_region = videos_per_region
        self.inject_errors = inject_errors
        self.calls: list[str] = []

    # ------------------------------------------------------------------ dispatch
    def __call__(self, url: str) -> dict:
        self.calls.append(url)
        parsed = urllib.parse.urlparse(url)
        params = dict(urllib.parse.parse_qsl(parsed.query))
        endpoint = parsed.path.rsplit("/", 1)[-1]
        if endpoint == "videoCategories":
            return self._categories(params["regionCode"])
        if endpoint == "videos":
            return self._videos(params)
        raise ValueError(f"Unsupported endpoint {endpoint}")

    # ---------------------------------------------------------------- endpoints
    def _categories(self, region: str) -> dict:
        items = [
            {
                "kind": "youtube#videoCategory",
                "etag": f"cat-{cid}",
                "id": cid,
                "snippet": {"title": title, "assignable": True, "channelId": "UCBR8-60-B28hp2BmDPdntcQ"},
            }
            for cid, title in CATEGORIES.items()
        ]
        return {"kind": "youtube#videoCategoryListResponse", "items": items}

    def _videos(self, params: dict) -> dict:
        region = params["regionCode"]
        page_size = int(params.get("maxResults", 5))
        offset = int(params.get("pageToken", "0") or 0)
        all_items = self._region_chart(region)
        page = all_items[offset : offset + page_size]
        body = {
            "kind": "youtube#videoListResponse",
            "items": page,
            "pageInfo": {"totalResults": len(all_items), "resultsPerPage": page_size},
        }
        if offset + page_size < len(all_items):
            body["nextPageToken"] = str(offset + page_size)
        return body

    # --------------------------------------------------------------- generation
    def _region_chart(self, region: str) -> list[dict]:
        rng = random.Random(f"{region}-{self.now:%Y%m%d}")
        hour_factor = 1 + self.now.hour / 24  # views grow through the day
        n_global = rng.randint(8, 15)
        ids = rng.sample(range(GLOBAL_POOL), n_global)
        ids += [
            10_000 + (zlib.crc32(region.encode()) % 10_000) * 1_000 + i
            for i in range(self.videos_per_region - n_global)
        ]
        rng.shuffle(ids)

        items = [self._video(n, rng, hour_factor) for n in ids[: self.videos_per_region]]

        if self.inject_errors and len(items) > 5:
            items[3]["snippet"]["publishedAt"] = "not-a-date"  # -> quarantine
            items[4]["statistics"].pop("viewCount")  # -> quarantine
            items[5]["statistics"].pop("likeCount")  # likes hidden: valid, nullable
            items.append(json.loads(json.dumps(items[0])))  # duplicate -> de-duplicated
        return items

    def _video(self, n: int, rng: random.Random, hour_factor: float) -> dict:
        vrng = random.Random(n)
        category_id = vrng.choice(list(CATEGORIES))
        published = self.now - timedelta(hours=vrng.randint(6, 96))
        views = int(vrng.lognormvariate(13, 1.2) * hour_factor * rng.uniform(0.9, 1.1))
        likes = int(views * vrng.uniform(0.01, 0.08))
        comments = int(likes * vrng.uniform(0.02, 0.15))
        title = f"Sample trending video #{n} - {CATEGORIES[category_id]}"
        return {
            "kind": "youtube#video",
            "etag": f"etag-{n}",
            "id": _video_id(n),
            "snippet": {
                "publishedAt": published.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "channelId": CHANNELS[n % len(CHANNELS)],
                "title": title,
                "description": "Synthetic description for local testing.",
                "channelTitle": f"Sample Channel {n % len(CHANNELS)}",
                "tags": [f"tag{t}" for t in range(vrng.randint(0, 8))],
                "categoryId": category_id,
                "liveBroadcastContent": "none",
                "defaultAudioLanguage": vrng.choice(["en", "hi", "ja", "de", "fr", "ko", "pt"]),
            },
            "contentDetails": {
                "duration": _duration(vrng.randint(30, 3 * 3600)),
                "dimension": "2d",
                "definition": vrng.choice(["hd", "hd", "hd", "sd"]),
                "caption": vrng.choice(["true", "false"]),
                "licensedContent": vrng.choice([True, False]),
            },
            "statistics": {
                "viewCount": str(views),
                "likeCount": str(likes),
                "favoriteCount": "0",
                "commentCount": str(comments),
            },
        }
