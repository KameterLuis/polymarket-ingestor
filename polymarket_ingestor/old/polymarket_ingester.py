#!/usr/bin/env python3
import json
import logging
import math
import os
import re
import signal
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Set, Tuple

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# pip install supabase
from supabase import Client, create_client

load_dotenv()

# -------- Config (via env) ----------
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY", "")
# SERVICE_ROLE_KEY preferred for upsert; if not, anon works if your RLS allows it.

DATA_API_TRADES = "https://data-api.polymarket.com/trades"

# Discovery knobs
DISCOVERY_CASH_MIN = float(
    os.environ.get("DISCOVERY_CASH_MIN", "200")
)  # only count fills >= this USDC
DISCOVERY_LOOKBACK_SEC = int(os.environ.get("DISCOVERY_LOOKBACK_SEC", "86400"))  # 24h
DISCOVERY_MAX_WALLETS = int(os.environ.get("DISCOVERY_MAX_WALLETS", "500"))

# Paging / pacing
PER_WALLET_PAGE_LIMIT = int(os.environ.get("PER_WALLET_PAGE_LIMIT", "1000"))
MAX_PAGES_PER_WALLET = int(os.environ.get("MAX_PAGES_PER_WALLET", "1000"))
DISCOVERY_PAGE_LIMIT = int(os.environ.get("DISCOVERY_PAGE_LIMIT", "1000"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "20"))

# Rate limits & backoff (documented /trades supports paging; we target low RPS + backoff)
MAX_RPS_TRADES = float(os.environ.get("MAX_RPS_TRADES", "3.0"))
BACKOFF_BASE = float(os.environ.get("BACKOFF_BASE", "1.0"))
BACKOFF_MAX = float(os.environ.get("BACKOFF_MAX", "20.0"))

# Main loop cadence
SLEEP_BETWEEN_WALLETS_SEC = float(os.environ.get("SLEEP_BETWEEN_WALLETS_SEC", "1.0"))
GLOBAL_LOOP_SLEEP_SEC = float(os.environ.get("GLOBAL_LOOP_SLEEP_SEC", "600"))

# -------- Logging & shutdown --------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("ingester")
_stop = False


def _handle(sig, frame):
    global _stop
    _stop = True


signal.signal(signal.SIGINT, _handle)
signal.signal(signal.SIGTERM, _handle)


# -------- Helpers --------
def supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError(
            "Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or ANON) env vars."
        )
    return create_client(SUPABASE_URL, SUPABASE_KEY)


class RateLimiter:
    def __init__(self, max_rps: float):
        self.min_interval = 1.0 / max_rps if max_rps > 0 else 0.0
        self._last = 0.0

    def wait(self):
        now = time.time()
        dt = now - self._last
        if dt < self.min_interval:
            time.sleep(self.min_interval - dt)
        self._last = time.time()


rl_trades = RateLimiter(MAX_RPS_TRADES)


def request_with_backoff(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params=None,
    timeout=REQUEST_TIMEOUT,
):
    attempt = 0
    while True:
        rl_trades.wait()
        try:
            resp = session.request(method, url, params=params, timeout=timeout)
            if resp.status_code < 400:
                return resp
            if resp.status_code in (429, 500, 502, 503, 504):
                attempt += 1
                back = min(BACKOFF_MAX, BACKOFF_BASE * (2 ** (attempt - 1)))
                back *= 0.5 + random.random() * 0.5  # jitter
                log.warning(
                    f"{resp.status_code} on {url} backoff {back:.2f}s (attempt {attempt})"
                )
                time.sleep(back)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            attempt += 1
            back = min(BACKOFF_MAX, BACKOFF_BASE * (2 ** (attempt - 1)))
            back *= 0.5 + random.random() * 0.5
            log.warning(f"Request error {e}; backoff {back:.2f}s (attempt {attempt})")
            time.sleep(back)


def utc_minute_bucket(ts_unix: int) -> datetime:
    dt = datetime.fromtimestamp(ts_unix, tz=timezone.utc)
    return dt.replace(second=0, microsecond=0)


def upsert_wallet(sb: Client, wallet: str):
    now = datetime.now(timezone.utc).isoformat()
    sb.table("wallets").upsert(
        {"wallet_address": wallet, "last_seen_at": now}
    ).execute()


# -------- Discovery: find active wallets via /trades filterType=CASH --------
def discover_active_wallets() -> List[str]:
    """
    Pull newest global trades in pages and collect wallets whose usdc notional >= DISCOVERY_CASH_MIN.
    Stop when we've crossed the lookback window or run out of data.
    """
    session = requests.Session()
    cutoff = int(time.time()) - DISCOVERY_LOOKBACK_SEC

    offset = 0
    found: Dict[str, float] = (
        {}
    )  # wallet -> sum usdc within window (approx. price*size)
    while True:
        params = {
            "limit": DISCOVERY_PAGE_LIMIT,
            "offset": offset,
            # key trick: only return trades above some CASH threshold
            "filterType": "CASH",
            "filterAmount": DISCOVERY_CASH_MIN,
        }
        r = request_with_backoff(session, "GET", DATA_API_TRADES, params=params)
        try:
            batch = r.json()
        except Exception:
            batch = None
        if not isinstance(batch, list) or not batch:
            break

        oldest_ts = None
        for t in batch:
            ts = int(t.get("timestamp") or 0)
            if oldest_ts is None or ts < oldest_ts:
                oldest_ts = ts
            if ts < cutoff:
                continue
            w = (t.get("proxyWallet") or "").lower()
            if not w:
                continue
            size = float(t.get("size") or 0.0)
            price = float(t.get("price") or 0.0)
            usdc = size * price
            if usdc >= DISCOVERY_CASH_MIN:
                found[w] = found.get(w, 0.0) + usdc

        offset += len(batch)

        # stop early if the page we just read is already older than our cutoff
        if oldest_ts is not None and oldest_ts < cutoff:
            break

        # small polite pause
        time.sleep(0.15)

        if len(found) >= DISCOVERY_MAX_WALLETS:
            break

    # take top wallets by discovered USDC in window
    wallets = [
        w
        for w, _ in sorted(found.items(), key=lambda kv: kv[1], reverse=True)[
            :DISCOVERY_MAX_WALLETS
        ]
    ]
    log.info(
        f"Discovered {len(wallets)} active wallet(s) in last {DISCOVERY_LOOKBACK_SEC//3600}h (threshold ≥ ${DISCOVERY_CASH_MIN})."
    )
    return wallets


# -------- Per-wallet crawl & aggregation --------
def fetch_trades_for_wallet(wallet: str) -> Iterable[dict]:
    session = requests.Session()
    offset = 0
    pages = 0
    while pages < MAX_PAGES_PER_WALLET:
        params = {"user": wallet, "limit": PER_WALLET_PAGE_LIMIT, "offset": offset}
        r = request_with_backoff(session, "GET", DATA_API_TRADES, params=params)
        try:
            batch = r.json()
        except Exception:
            batch = None
        if not isinstance(batch, list) or not batch:
            break
        for t in batch:
            yield t
        offset += len(batch)
        pages += 1
        time.sleep(0.1)


def fold_minute_rollups(trades: Iterable[dict]) -> List[dict]:
    roll: Dict[Tuple[str, str, str, datetime], dict] = {}
    for t in trades:
        try:
            wallet = (t.get("proxyWallet") or "").lower()
            condition_id = t.get("conditionId") or ""
            outcome = t.get("outcome") or str(t.get("outcomeIndex") or "")
            side = (t.get("side") or "").upper()
            size = float(t.get("size") or 0.0)
            price = float(t.get("price") or 0.0)
            ts = int(t.get("timestamp") or 0)
            market_title = t.get("title") or None
            event_slug = t.get("slug") or t.get("eventSlug") or None
            if not wallet or not condition_id or ts <= 0 or size <= 0:
                continue

            bucket = utc_minute_bucket(ts)
            key = (wallet, condition_id, outcome, bucket)
            row = roll.get(key)
            if not row:
                row = roll[key] = {
                    "wallet_address": wallet,
                    "condition_id": condition_id,
                    "outcome": outcome,
                    "ts_minute": bucket.isoformat(),
                    "buy_qty": 0.0,
                    "buy_notional": 0.0,
                    "sell_qty": 0.0,
                    "sell_notional": 0.0,
                    "net_qty": 0.0,
                    "net_notional": 0.0,
                    "last_fill_at": datetime.fromtimestamp(
                        ts, tz=timezone.utc
                    ).isoformat(),
                    "market_title": market_title,
                    "event_slug": event_slug,
                }
            notional = size * price
            if side == "BUY":
                row["buy_qty"] += size
                row["buy_notional"] += notional
            elif side == "SELL":
                row["sell_qty"] += size
                row["sell_notional"] += notional
            row["net_qty"] = row["buy_qty"] - row["sell_qty"]
            row["net_notional"] = row["buy_notional"] - row["sell_notional"]
            # keep latest fill time
            if datetime.fromisoformat(
                row["last_fill_at"].replace("Z", "+00:00")
            ) < datetime.fromtimestamp(ts, tz=timezone.utc):
                row["last_fill_at"] = datetime.fromtimestamp(
                    ts, tz=timezone.utc
                ).isoformat()
            if not row.get("market_title") and market_title:
                row["market_title"] = market_title
            if not row.get("event_slug") and event_slug:
                row["event_slug"] = event_slug
        except Exception:
            continue

    # compute vwap fields
    for row in roll.values():
        row["buy_vwap"] = (
            (row["buy_notional"] / row["buy_qty"]) if row["buy_qty"] > 0 else None
        )
        row["sell_vwap"] = (
            (row["sell_notional"] / row["sell_qty"]) if row["sell_qty"] > 0 else None
        )
    return list(roll.values())


def upsert_rollups(sb: Client, rows: List[dict]):
    if not rows:
        return
    CHUNK = 500
    for i in range(0, len(rows), CHUNK):
        sb.table("trades_minute_rollup").upsert(
            rows[i : i + CHUNK],
            on_conflict="wallet_address,condition_id,outcome,ts_minute",
        ).execute()


def upsert_wallet(sb: Client, wallet: str):
    now = datetime.now(timezone.utc).isoformat()
    sb.table("data_wallets").upsert(
        {"wallet_address": wallet, "last_seen_at": now}
    ).execute()


# -------- Main --------
def main():
    sb = supabase_client()
    while not _stop:
        wallets = discover_active_wallets()
        if not wallets:
            log.info("No wallets discovered this cycle.")
            time.sleep(GLOBAL_LOOP_SLEEP_SEC)
            continue

        for w in wallets:
            if _stop:
                break
            try:
                upsert_wallet(sb, w)
                log.info(f"Crawling wallet {w} …")
                rows = fold_minute_rollups(fetch_trades_for_wallet(w))
                upsert_rollups(sb, rows)
                log.info(f"Upserted {len(rows)} rollup rows for {w}.")
            except Exception as e:
                log.warning(f"Wallet {w} failed: {e}")
            time.sleep(SLEEP_BETWEEN_WALLETS_SEC)

        log.info("Cycle complete. Sleeping…")
        time.sleep(GLOBAL_LOOP_SLEEP_SEC)


if __name__ == "__main__":
    main()
