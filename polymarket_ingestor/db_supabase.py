from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

from supabase import Client, create_client

UTC = timezone.utc


# Table names
TBL_M_1M = "market_candles_1m"
TBL_M_1H = "market_candles_1h"
TBL_M_1D = "market_candles_1d"
TBL_MARKETS = "markets"
TBL_EVENTS = "events"
TBL_LASTVOL = "market_last_cumvolume"


def _encode_for_postgrest(obj):
    """Recursively convert datetimes to ISO strings so httpx/json can serialize."""
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=UTC)
        return obj.astimezone(UTC).isoformat()
    if isinstance(obj, list):
        return [_encode_for_postgrest(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _encode_for_postgrest(v) for k, v in obj.items()}
    return obj


def connect(url: str, key: str) -> Client:
    return create_client(url, key)


async def upsert_markets(sb, payload: list[dict]):
    payload = _encode_for_postgrest(payload)  # <— serialize datetimes
    sb.table(TBL_MARKETS).upsert(payload, on_conflict="id").execute()


async def upsert_events(sb: Client, rows: Iterable[tuple]):
    keys = [
        "id",
        "active",
        "title",
        "slug",
        "description",
        "tags",
        "image",
        "startDate",
        "endDate",
        "competitive",
        "liquidity",
        "volume",
        "volume24hr",
        "volume1wk",
        "volume1mo",
        "volume1yr",
        "markets",
    ]
    payload = [dict(zip(keys, _serialize_row(r))) for r in rows]
    sb.table(TBL_EVENTS).upsert(payload, on_conflict="id").execute()


async def upsert_minutes(sb: Client, rows: Iterable[tuple]):
    keys = ["market_id", "ts", "price", "volume"]
    payload = [dict(zip(keys, _serialize_row(r))) for r in rows]
    sb.table(TBL_M_1M).upsert(payload, on_conflict="market_id,ts").execute()


async def upsert_hours(sb: Client, rows: Iterable[tuple]):
    keys = ["market_id", "ts", "price", "volume"]
    payload = [dict(zip(keys, _serialize_row(r))) for r in rows]
    sb.table(TBL_M_1H).upsert(payload, on_conflict="market_id,ts").execute()


async def upsert_days(sb: Client, rows: Iterable[tuple]):
    keys = ["market_id", "ts", "price", "volume"]
    payload = [dict(zip(keys, _serialize_row(r))) for r in rows]
    sb.table(TBL_M_1D).upsert(payload, on_conflict="market_id,ts").execute()


async def get_last_cum(sb: Client, market_id: int) -> float | None:
    res = (
        sb.table(TBL_LASTVOL)
        .select("last_cum_volume")
        .eq("market_id", market_id)
        .maybe_single()
        .execute()
    )
    data = getattr(res, "data", None)
    if data and isinstance(data, dict) and data.get("last_cum_volume") is not None:
        return float(data["last_cum_volume"])
    return None


async def set_last_cum(sb: Client, market_id: int, v: float):
    sb.table(TBL_LASTVOL).upsert(
        {"market_id": market_id, "last_cum_volume": v}
    ).execute()


async def fetch_minutes_range(
    sb: Client, start: datetime, end: datetime, page_size: int = 50000
) -> List[Dict[str, Any]]:
    start_iso = _iso(start)
    end_iso = _iso(end)
    out: List[Dict[str, Any]] = []
    from_idx = 0
    while True:
        to_idx = from_idx + page_size - 1
        res = (
            sb.table(TBL_M_1M)
            .select("market_id,ts,price,volume")
            .gte("ts", start_iso)
            .lt("ts", end_iso)
            .order("ts", desc=False)
            .range(from_idx, to_idx)
            .execute()
        )
        items = getattr(res, "data", []) or []
        out.extend(items)
        if len(items) < page_size:
            break
        from_idx += page_size
    return out


def _iso(dt: datetime | None):
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat()


def _serialize_row(row: tuple):
    out = []
    for v in row:
        if isinstance(v, datetime):
            out.append(_iso(v))
        elif isinstance(v, (list, dict)):
            out.append(
                v
            )  # supabase will JSON-encode arrays/objects if column types match
        else:
            out.append(v)
    return out
