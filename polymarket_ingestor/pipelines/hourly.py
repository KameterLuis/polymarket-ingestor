from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .. import db_supabase

UTC = timezone.utc


async def rollup(conn, end_ts: datetime):
    end = end_ts.replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    await db_supabase.rollup_hour(conn, start, end)
