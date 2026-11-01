from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Literal

if TYPE_CHECKING:
    from pfeed.base.feed import BaseFeed

import httpx2
from pfeed.dataflow.retry_policy import RetryPolicy
from pfeed.enums import (
    DataAccessType,
    DataCategory,
    DataProviderType,
    DataType,
)
from pfeed.source import BaseSource, SourceMetadata
from pfund_blocks.enums.env import Environment
from pfund_bybit.adapter import BybitAdapter

from pfeed_bybit.batch_api import BatchAPI
from pfeed_bybit.market.feed import BybitMarketFeed
from pfeed_bybit.stream_api import StreamAPI


class Bybit(BaseSource):
    Feeds: ClassVar[dict[DataCategory, type[BaseFeed]]] = {
        DataCategory.MARKET_DATA: BybitMarketFeed,
    }
    metadata: ClassVar[SourceMetadata] = SourceMetadata(
        name="BYBIT",
        url="https://www.bybit.com",
        data_categories={
            DataCategory.MARKET_DATA: {
                DataType.TICK: [
                    "PERPETUAL",
                    "INVERSE-PERPETUAL",
                    "FUTURE",
                    "INVERSE-FUTURE",
                    "CRYPTO",
                ],
            },
        },
        provider_type=DataProviderType.VENUE,
        access_type=DataAccessType.FREE,
        start_date="2020-01-01",
    )
    # timeouts and connection errors; HTTP error responses (e.g. 5xx) are not retried
    retry_policy: ClassVar[RetryPolicy] = RetryPolicy(retry_on=(httpx2.TransportError,))
    # Bybit's symbol conventions come from pfund-bybit's adapter, shared with pfund's Bybit venue;
    # products are created by BaseSource.create_product() with it
    adapter: ClassVar[BybitAdapter] = BybitAdapter()  # ty: ignore[invalid-mutable-override]

    market_feed: BybitMarketFeed

    def __init__(
        self,
        pipeline_mode: bool = False,
        num_workers: int | dict[DataCategory | str, int] | None = None,
    ):
        # APIs are set before super().__init__(), which creates the feeds
        self._batch_api: BatchAPI | None = None
        self._stream_api: StreamAPI | None = None
        super().__init__(pipeline_mode=pipeline_mode, num_workers=num_workers)

    def get_batch_api(self) -> BatchAPI:
        if self._batch_api is None:
            self._batch_api = BatchAPI()
        return self._batch_api

    def get_stream_api(
        self,
        env: Literal[
            Environment.PAPER,
            Environment.LIVE,
            "PAPER",
            "LIVE",
        ],
    ) -> StreamAPI:
        environment = Environment(env.upper())
        if environment not in (Environment.PAPER, Environment.LIVE):
            raise ValueError(f"{self.name} streaming only supports PAPER and LIVE, got {environment}")
        if self._stream_api is None:
            self._stream_api = StreamAPI(env=environment)
        elif self._stream_api.env != environment:
            raise ValueError(
                f"{self.name} stream API is already in {self._stream_api.env}, cannot stream in {environment}"
            )
        return self._stream_api
