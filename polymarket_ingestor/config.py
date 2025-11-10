from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    supabase_url: str
    supabase_key: str
    gamma_base: str = "https://gamma-api.polymarket.com"
    reqs_per_window: int = 10
    window_seconds: int = 10
    bulk_size: int = 500
    retention_minute_minutes: int = 60
    retention_hour_days: int = 7

    s3_bucket: str | None = None
    s3_region: str | None = None
    s3_prefix: str = "polymarket/"

    @staticmethod
    def from_env() -> "Config":
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            raise RuntimeError("SUPABASE_URL / SUPABASE_KEY missing")
        return Config(
            supabase_url=url,
            supabase_key=key,
            gamma_base=os.environ.get("GAMMA_BASE", "https://gamma-api.polymarket.com"),
            s3_bucket=os.environ.get("S3_BUCKET"),
            s3_region=os.environ.get("S3_REGION"),
            s3_prefix=os.environ.get("S3_PREFIX", "polymarket/"),
        )
