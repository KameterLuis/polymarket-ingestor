from datetime import datetime, timezone, timedelta

UTC = timezone.utc

def _events_to_redis_pipeline(pipe, events: list[dict]):
    """Populates a Redis pipeline with commands to cache all events."""
    print(f"  > Preparing {len(events)} events for Redis cache...")
    for event in events:
        try:
            # The key for each event (e.g., "event:123")
            key = f"event:{event['id']}"
            value = json.dumps(event)
            # Use the same 60-minute expiry
            pipe.set(key, value, ex=MARKET_CACHE_TTL_SECONDS)
        except Exception as e:
            print(f"  > Error preparing event {event.get('id')} for Redis: {e}")
    return pipe

async def seconds_until_next_minute(now=None) -> float:
    now = now or datetime.now(tz=UTC)
    nxt = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    return max((nxt - now).total_seconds(), 0.5)

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

def _json_parse(x):
    if x is None:
        return None
    if isinstance(x, (list, dict)):
        return x
    if isinstance(x, str):
        s = x.strip()
        if s and (s[0] in "[{" and s[-1] in "]}"):
            try:
                import json
                return json.loads(s)
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
            return datetime.fromisoformat(x.replace("Z", "+00:00")).astimezone(UTC)
        except Exception:
            return None
    if isinstance(x, datetime):
        return x.astimezone(UTC) if x.tzinfo else x.replace(tzinfo=UTC)
    return None

def _as_array_of_numbers(x):
    arr = _json_parse(x)
    if arr is None:
        # If API returned nothing, use empty list (works with jsonb NOT NULL)
        return []
    if isinstance(arr, list):
        out = []
        for v in arr:
            try:
                out.append(float(v))
            except Exception:
                out.append(v)  # keep as-is if not numeric
        return out
    # scalar -> wrap
    try:
        return [float(arr)]
    except Exception:
        return [arr]