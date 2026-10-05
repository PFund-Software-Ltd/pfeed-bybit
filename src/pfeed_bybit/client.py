from __future__ import annotations

from typing import ClassVar

from pfeed.client import DataClient
from pfeed.enums import DataCategory
from pfeed.feeds.base_feed import BaseFeed
from pfeed_bybit.source import BybitSource
from pfeed_bybit.feeds.market_feed import BybitMarketFeed


class Bybit(DataClient):
    DataSource: ClassVar[type[BybitSource]] = BybitSource
    Feeds: ClassVar[dict[DataCategory, type[BaseFeed]]] = {
        DataCategory.MARKET_DATA: BybitMarketFeed,
    }
    data_source: BybitSource

    market_feed: BybitMarketFeed
