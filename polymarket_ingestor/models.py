from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class Market:
    id: int
    active: bool
    question: str | None
    description: str | None
    slug: str | None
    image: str | None
    startDate: datetime | None
    endDate: datetime | None
    outcomes: Any
    outcomePrice: Any
    oneHourPrice: float
    oneDayPrice: float
    oneMonthPrice: float
    oneWeekPrice: float
    lastTradedPrice: float
    bestAsk: float
    bestBid: float
    volume: float
    volume24hr: float
    volume1wk: float
    volume1mo: float
    volume1yr: float
    liquidity: float
    events: list[int] | None


@dataclass
class Event:
    id: int
    active: bool
    title: str | None
    slug: str | None
    description: str | None
    tags: Any
    image: str | None
    startDate: datetime | None
    endDate: datetime | None
    competitive: float
    liquidity: float
    volume: float
    volume24hr: float
    volume1wk: float
    volume1mo: float
    volume1yr: float
    markets: list[int] | None
