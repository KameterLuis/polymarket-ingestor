from __future__ import annotations

import asyncio

# scheduler.py
from datetime import datetime, timedelta, timezone

from aiolimiter import AsyncLimiter

from . import db_supabase as db
from .config import Config
from .gamma_client import GammaClient
from .pipelines import daily as daily_pipeline
from .pipelines import hourly as hourly_pipeline
from .pipelines import minute as minute_pipeline
from .storage.s3 import archive_and_prune_minutes

UTC = timezone.utc

import json


def _json_or_none(x):
    # pass lists/dicts through (for jsonb columns); stringify scalars; keep None
    if x is None:
        return None
    if isinstance(x, (list, dict)):
        return x
    # strings from API are fine as-is for jsonb; but if your column is TEXT, keep as string
    return x


def _events_list(m):
    v = m.get("events")
    if isinstance(v, list):
        out = []
        for item in v:
            if isinstance(item, dict) and "id" in item:
                try:
                    out.append(int(item["id"]))
                except Exception:
                    continue
            else:
                try:
                    out.append(int(item))
                except Exception:
                    continue
        return out or None
    if m.get("eventId") is not None:
        try:
            return [int(m["eventId"])]
        except Exception:
            return None
    return None


def _ts(x):
    """Normalize Gamma timestamps to tz-aware datetime (UTC).
    Accepts ISO strings, ms/seconds epoch, or datetime; returns datetime|None.
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        # if milliseconds epoch
        if x > 1e12:
            x = x / 1000.0
        return datetime.fromtimestamp(float(x), tz=UTC)
    if isinstance(x, str):
        try:
            # handle ...Z and offsetless ISO
            return datetime.fromisoformat(x.replace("Z", "+00:00")).astimezone(UTC)
        except Exception:
            return None
    if isinstance(x, datetime):
        return x.astimezone(UTC) if x.tzinfo else x.replace(tzinfo=UTC)
    return None


async def seconds_until_next_minute(now=None) -> float:
    now = now or datetime.now(tz=UTC)
    nxt = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    return max((nxt - now).total_seconds(), 0.5)


async def run_once(cfg: Config, sb, client: GammaClient):
    # ids = await client.fetch_active_market_ids()
    # markets = await client.fetch_markets_bulk(ids, cfg.bulk_size)
    markets = await client.fetch_markets_all(closed="false")

    # Build explicit dicts to avoid misalignment
    market_payload = []
    for m in markets:
        market_payload.append(
            {
                "id": int(m.get("id") or m.get("marketId")),
                "active": bool(m.get("active", True)),
                "question": m.get("question"),
                "description": m.get("description"),
                "slug": m.get("slug"),
                "image": m.get("image"),
                "startDate": _ts(m.get("startDate")),
                "endDate": _ts(m.get("endDate")),
                # match your Supabase column names exactly:
                "outcomes": _json_or_none(m.get("outcomes")),  # JSONB or TEXT ok
                "outcomePrices": _json_or_none(
                    m.get("outcomePrices") or m.get("outcomePrice")
                ),
                "oneHourPriceChange": float(m.get("oneHourPriceChange", 0) or 0),
                "oneDayPriceChange": float(m.get("oneDayPriceChange", 0) or 0),
                "oneMonthPriceChange": float(m.get("oneMonthPriceChange", 0) or 0),
                "oneWeekPriceChange": float(m.get("oneWeekPriceChange", 0) or 0),
                "lastTradedPrice": float(
                    m.get("lastTradePrice", m.get("lastTradedPrice", 0)) or 0
                ),
                "bestAsk": float(m.get("bestAsk", 0) or 0),
                "bestBid": float(m.get("bestBid", 0) or 0),
                "volume": float(m.get("volume", m.get("volumeNum", 0)) or 0),
                "volume24hr": float(m.get("volume24hr", 0) or 0),
                "volume1wk": float(m.get("volume1wk", 0) or 0),
                "volume1mo": float(m.get("volume1mo", 0) or 0),
                "volume1yr": float(m.get("volume1yr", 0) or 0),
                "liquidity": float(m.get("liquidity", m.get("liquidityNum", 0)) or 0),
                # this must be int4[] in Supabase
                "events": _events_list(m),
            }
        )

    await db.upsert_markets(sb, market_payload)

    # Upsert events
    events = await client.fetch_events()
    await db.upsert_events(
        sb,
        (
            (
                int(e.get("id")),
                bool(e.get("active", True)),
                e.get("title"),
                e.get("slug"),
                e.get("description"),
                e.get("tags"),
                e.get("image"),
                _ts(e.get("startDate")),
                _ts(e.get("endDate")),
                float(e.get("competitive", 0) or 0),
                float(e.get("liquidity", 0) or 0),
                float(e.get("volume", 0) or 0),
                float(e.get("volume24hr", 0) or 0),
                float(e.get("volume1wk", 0) or 0),
                float(e.get("volume1mo", 0) or 0),
                float(e.get("volume1yr", 0) or 0),
                _markets_list(e),
            )
            for e in events
        ),
    )

    # Minute time series
    await minute_pipeline.write_minute(sb, markets, datetime.now(tz=UTC))


def _events_list(m):
    v = m.get("events")
    if isinstance(v, list):
        out = []
        for item in v:
            if isinstance(item, dict) and "id" in item:
                try:
                    out.append(int(item["id"]))
                except Exception:
                    continue
            else:
                # already a scalar ID
                try:
                    out.append(int(item))
                except Exception:
                    continue
        return out or None
    if m.get("eventId") is not None:
        try:
            return [int(m["eventId"])]
        except Exception:
            return None
    return None


def _markets_list(e):
    v = e.get("markets")
    if isinstance(v, list):
        try:
            return [int(x) for x in v]
        except Exception:
            return None
    if isinstance(v, (int, float)):
        return [int(v)]
    return None


async def loop(cfg: Config):
    limiter = AsyncLimiter(cfg.reqs_per_window, cfg.window_seconds)
    client = GammaClient(cfg.gamma_base, limiter)
    sb = db.connect(cfg.supabase_url, cfg.supabase_key)

    try:
        while True:
            now = datetime.now(tz=UTC)
            await run_once(cfg, sb, client)

            # rollups
            if now.minute == 0:
                await hourly_pipeline.rollup(sb, now)
            if now.hour == 0 and now.minute == 0:
                await daily_pipeline.rollup(sb, now)

            # archive + prune hot data (optional, enable when ready)
            # await archive_and_prune_minutes(sb, table=db.TBL_M_1M,
            # older_than=now - timedelta(minutes=cfg.retention_minute_minutes),
            # bucket=cfg.s3_bucket, region=cfg.s3_region, prefix=cfg.s3_prefix)
            # await archive_and_prune_minutes(sb, table=db.TBL_M_1H,
            # older_than=now - timedelta(days=cfg.retention_hour_days),
            # bucket=cfg.s3_bucket, region=cfg.s3_region, prefix=cfg.s3_prefix)

            await asyncio.sleep(await seconds_until_next_minute())
    finally:
        await client.close()
