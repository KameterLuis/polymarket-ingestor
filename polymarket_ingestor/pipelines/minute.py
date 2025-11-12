from __future__ import annotations
from datetime import datetime, timezone
from typing import Dict, Iterable
from .. import db_supabase as db

UTC = timezone.utc
PRICE_KEYS = ("lastTradedPrice", "lastTradePrice", "lastPrice", "price")

def _pick_price(m: Dict) -> float:
    for k in PRICE_KEYS:
        v = m.get(k)
        if v is not None:
            try: return float(v)
            except: pass
    try:
        bid = float(m.get("bestBid", 0) or 0)
        ask = float(m.get("bestAsk", 0) or 0)
        if bid and ask:
            return (bid + ask) / 2.0
    except: pass
    return 0.0

def _pick_cumulative_volume(m: Dict) -> float:
    for k in ("volume", "volumeNum", "totalVolume", "lifetimeVolume"):
        v = m.get(k)
        if v is not None:
            try: return float(v)
            except: pass
    return 0.0

async def write_minute(sb, markets: Iterable[Dict], now_ts: datetime):
    """Write one minute bucket per market: price=last, volume=delta(lifetime)."""
    bucket = now_ts.replace(second=0, microsecond=0, tzinfo=UTC)
    rows = []
    for m in markets:
        mid = m.get("id") or m.get("marketId")
        if mid is None: 
            continue
        mid = int(mid)
        price = _pick_price(m)
        cum = _pick_cumulative_volume(m)
        last = await db.get_last_cum(sb, mid)
        delta = 0.0 if last is None else max(cum - last, 0.0)
        await db.set_last_cum(sb, mid, cum)
        rows.append((mid, bucket, price, delta))
    if rows:
        await db.upsert_minutes(sb, rows)
