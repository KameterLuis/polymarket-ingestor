from __future__ import annotations

from typing import Any, Dict, List

import httpx
from aiolimiter import AsyncLimiter


class GammaClient:
    def __init__(self, base_url: str, limiter: AsyncLimiter):
        self.base_url = base_url.rstrip("/")
        self.limiter = limiter
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30)

    async def close(self):
        await self.client.aclose()

    async def _get(self, path: str, params: Dict[str, Any] | None = None) -> Any:
        async with self.limiter:
            r = await self.client.get(path, params=params)
        r.raise_for_status()
        return r.json()

    async def fetch_active_market_ids(self) -> List[int]:
        ids: List[int] = []
        limit = 1000
        offset = 0
        while True:
            data = await self._get(
                "/markets",
                {
                    "closed": "false",
                    "limit": limit,
                    "offset": offset,
                    "order": "volume",
                    "ascending": "true",
                },
            )
            items = data if isinstance(data, list) else data.get("data", [])
            if not items:
                break
            for m in items:
                mid = m.get("id") or m.get("marketId")
                if mid is not None:
                    ids.append(int(mid))
            if len(items) < limit:
                break
            offset += limit
        return sorted(set(ids))

    async def fetch_markets_all(
        self, closed="false", order="volume", ascending="true"
    ) -> list[dict]:
        out = []
        limit, offset = 500, 0
        while True:
            data = await self._get(
                "/markets",
                {
                    "closed": str(closed).lower(),
                    "limit": limit,
                    "offset": offset,
                    "order": order,
                    "ascending": str(ascending).lower(),
                },
            )
            items = data if isinstance(data, list) else data.get("data", [])
            if not items:
                break
            out.extend(items)
            if len(items) < limit:
                break
            offset += limit
        return out

    async def fetch_markets_bulk(self, ids: List[int], bulk_size: int) -> List[dict]:
        out: List[dict] = []
        for i in range(0, len(ids), bulk_size):
            chunk = ids[i : i + bulk_size]
            try:
                data = await self._get(
                    "/markets/bulk", {"ids": ",".join(map(str, chunk))}
                )
                items = data if isinstance(data, list) else data.get("data", [])
                out.extend(items)
            except httpx.HTTPStatusError:
                for mid in chunk:
                    d = await self._get("/markets", {"ids": str(mid)})
                    if isinstance(d, list) and d:
                        out.append(d[0])
                    elif isinstance(d, dict) and d.get("data"):
                        out.append(d["data"][0])
        return out

    async def fetch_events(self) -> List[dict]:
        out: List[dict] = []
        limit = 500
        offset = 0
        while True:
            data = await self._get("/events", {"limit": limit, "offset": offset})
            items = data if isinstance(data, list) else data.get("data", [])
            if not items:
                break
            out.extend(items)
            if len(items) < limit:
                break
            offset += limit
        return out
