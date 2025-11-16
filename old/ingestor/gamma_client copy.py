from typing import Any, Dict, List

import httpx
import time
from aiolimiter import AsyncLimiter

class GammaClient:
    def __init__(self):
        self.base_url = "https://gamma-api.polymarket.com"
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30)

    async def close(self):
        await self.client.aclose()

    async def _get(self, path: str, params: Dict[str, Any] | None = None) -> Any:
        r = await self.client.get(path, params=params)
        r.raise_for_status()
        return r.json()

    async def fetch_markets(
        self
    ) -> list[dict]:
        out = []
        limit, offset = 500, 0
        print("fetching markets")
        #for i in range(0, 1):
        while True:
            data = await self._get(
                "/markets",
                {
                    "closed": "false",
                    "limit": limit,
                    "offset": offset,
                },
            )
            items = data if isinstance(data, list) else data.get("data", [])
            if not items:
                break
            out.extend(items)
            if len(items) < limit:
                break
            offset += limit
        print("collected ", len(out), " markets")
        return out

    async def fetch_events(self) -> List[dict]:
        out: List[dict] = []
        limit = 500
        offset = 0
        print("fetching events")
        #for i in range(0, 1):
        while True:
            data = await self._get("/events", {"limit": limit, "offset": offset, "closed": "false",})
            items = data if isinstance(data, list) else data.get("data", [])
            if not items:
                break
            out.extend(items)
            if len(items) < limit:
                break
            offset += limit
        print("collected ", len(out), " events")
        return out