from __future__ import annotations

import asyncio
import random
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx
from aiolimiter import AsyncLimiter


class GammaClient:
    """
    Fast client for Polymarket Gamma API with concurrent pagination.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://gamma-api.polymarket.com",
        # HTTP settings
        max_connections: int = 40,
        max_keepalive_connections: int = 20,
        timeout_connect: float = 5.0,
        timeout_read: float = 30.0,
        timeout_write: float = 5.0,
        timeout_pool: float = 5.0,
        # Concurrency for pagination
        default_concurrency: int = 12,
        default_batch_pages: int = 12,
        # Optional rate limit (requests, per_seconds). Example: (125, 10)
        rate_limit: Optional[Tuple[int, int]] = None,
        # Retries
        max_retries: int = 5,
    ) -> None:
        self.base_url = base_url
        self.default_concurrency = default_concurrency
        self.default_batch_pages = default_batch_pages
        self.max_retries = max_retries

        self._limits = httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
        )
        self._timeout = httpx.Timeout(
            connect=timeout_connect,
            read=timeout_read,
            write=timeout_write,
            pool=timeout_pool,
        )
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            http2=True,
            limits=self._limits,
            timeout=self._timeout,
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
                "User-Agent": "arked-fetch/1.0",
            },
        )
        self._limiter: Optional[AsyncLimiter] = (
            AsyncLimiter(rate_limit[0], time_period=rate_limit[1]) if rate_limit else None
        )

    # -------- lifecycle --------
    async def close(self) -> None:
        await self.client.aclose()

    async def __aenter__(self) -> "GammaClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # -------- low-level GET with retry/backoff --------
    async def _get(self, path: str, params: Dict[str, Any] | None = None) -> Any:
        """
        GET with retries on 429/5xx. Returns parsed JSON.
        """
        # strip None values from params
        if params:
            params = {k: v for k, v in params.items() if v is not None}

        backoff = 0.2
        for attempt in range(self.max_retries):
            if self._limiter:
                async with self._limiter:
                    resp = await self.client.get(path, params=params)
            else:
                resp = await self.client.get(path, params=params)

            # retry on transient codes
            if resp.status_code in (429, 500, 502, 503, 504):
                # honor Retry-After if present
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        delay = backoff
                else:
                    # jitter to avoid thundering herd
                    delay = backoff + random.uniform(0, backoff / 2)
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(delay)
                    backoff = min(backoff * 2, 2.0)
                    continue

            resp.raise_for_status()
            return resp.json()

        # if we somehow drop out, raise last status
        resp.raise_for_status()  # type: ignore[name-defined]
        return None  # unreachable

    @staticmethod
    def _items_from_response(data: Any) -> List[dict]:
        """
        Gamma sometimes returns {"data": [...]} and sometimes a bare list.
        """
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            payload = data.get("data")
            if isinstance(payload, list):
                return payload
        return []

    # -------- generic, fast paginated fetch --------
    async def _fetch_paginated(
        self,
        path: str,
        *,
        limit: int = 500,
        base_params: Optional[Dict[str, Any]] = None,
        # pagination stability (if the API supports these):
        order_by: Optional[str] = "id",
        ascending: Optional[bool] = False,
        # concurrency
        concurrency: Optional[int] = None,
        batch_pages: Optional[int] = None,
        verbose: bool = True,
    ) -> List[dict]:
        """
        Fetches all pages by firing off 'batch_pages' requests at a time with a cap
        of 'concurrency' in-flight requests. Stops when a page < limit is seen.

        Uses offset pagination. To keep results stable if new items arrive, we
        pass order + ascending when supported by the endpoint.
        """
        if concurrency is None:
            concurrency = self.default_concurrency
        if batch_pages is None:
            batch_pages = self.default_batch_pages

        sem = asyncio.Semaphore(concurrency)
        out: List[dict] = []
        offset = 0

        # shared base params
        base_params = dict(base_params or {})
        base_params.setdefault("limit", limit)
        base_params.setdefault("offset", 0)
        if order_by:
            base_params.setdefault("order", order_by)
        if ascending is not None:
            base_params.setdefault("ascending", "true" if ascending else "false")

        if verbose:
            print(f"Fetching {path} with limit={limit}, concurrency={concurrency}, batch_pages={batch_pages}")

        async def fetch_page(off: int) -> Tuple[int, List[dict]]:
            async with sem:
                params = dict(base_params)
                params["offset"] = off
                data = await self._get(path, params=params)
                items = self._items_from_response(data)
                return off, items

        while True:
            # build a wave of offsets
            offsets = [offset + i * limit for i in range(batch_pages)]
            tasks = [asyncio.create_task(fetch_page(o)) for o in offsets]
            results = await asyncio.gather(*tasks, return_exceptions=False)

            # keep deterministic order by offset
            results.sort(key=lambda t: t[0])

            last_len = 0
            for _, items in results:
                if not items:
                    last_len = 0
                    break
                out.extend(items)
                last_len = len(items)

            if last_len < limit:
                break
            offset += batch_pages * limit

        if verbose:
            print(f"Collected {len(out)} items from {path}")
        return out

    # -------- public helpers --------
    async def fetch_markets(
        self,
        *,
        limit: int = 500,
        closed: bool = False,
        concurrency: Optional[int] = None,
        batch_pages: Optional[int] = None,
        verbose: bool = True,
    ) -> List[dict]:
        """
        Fetch all markets quickly.
        """
        params = {
            "closed": "false" if not closed else "true",
        }
        return await self._fetch_paginated(
            "/markets",
            limit=limit,
            base_params=params,
            order_by="id",
            ascending=False,
            concurrency=concurrency,
            batch_pages=batch_pages,
            verbose=verbose,
        )

    async def fetch_events(
        self,
        *,
        limit: int = 500,
        closed: bool = False,
        concurrency: Optional[int] = None,
        batch_pages: Optional[int] = None,
        verbose: bool = True,
    ) -> List[dict]:
        """
        Fetch all events quickly.
        """
        params = {
            "closed": "false" if not closed else "true",
        }
        return await self._fetch_paginated(
            "/events",
            limit=limit,
            base_params=params,
            order_by="id",
            ascending=False,
            concurrency=concurrency,
            batch_pages=batch_pages,
            verbose=verbose,
        )
