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

    @staticmethod
    def from_env() -> "Config":
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            raise RuntimeError("SUPABASE_URL / SUPABASE_KEY missing")
        return Config(
            supabase_url=url,
            supabase_key=key,
        )