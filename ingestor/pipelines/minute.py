from __future__ import annotations
from datetime import datetime, timezone
from typing import Dict, Iterable
from .. import db_supabase as db
import math

UTC = timezone.utc

def _pick_price(m: Dict) -> float:
    v = m.get("lastTradePrice", 0.0)
    try:
        v = float(v)
    except Exception:
        return 0.0
    if math.isnan(v) or math.isinf(v):
        return 0.0
    return v

def _pick_volume(m: Dict) -> float:
    v = m.get("volumeNum", 0.0)
    try:
        v = float(v)
    except Exception:
        return 0.0
    if math.isnan(v) or math.isinf(v):
        return 0.0
    return v

async def write_minute(sb, markets: Iterable[Dict], now_ts: datetime):
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    rows = []
    for m in markets:
        mid = m.get("id") or m.get("marketId")
        if mid is None: 
            continue
        mid = int(mid)
        price = _pick_price(m)
        vol = _pick_volume(m)
        if vol is None:
            vol = 0.0
        rows.append({"market_id": mid, "ts": now_iso, "price": price, "volume": vol})
    if rows:
        await db.upsert_minutes(sb, rows)
        return rows
