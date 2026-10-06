# pyright: reportArgumentType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal

if TYPE_CHECKING:
    from pfund.venues.bybit.product import BybitProduct

from pfund.enums.env import Environment

from pfeed.enums import (
    DataAccessType,
    DataCategory,
    DataProviderType,
    DataType,
)
from pfeed.source import BaseSource, SourceMetadata
from pfeed_bybit.batch_api import BatchAPI
from pfeed_bybit.stream_api import StreamAPI


class BybitSource(BaseSource):
    METADATA: ClassVar[SourceMetadata] = SourceMetadata(
        name="BYBIT",
        data_origin="https://www.bybit.com",
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
        feed_capabilities={DataCategory.MARKET_DATA: {"download", "stream"}},
        provider_type=DataProviderType.VENUE,
        access_type=DataAccessType.FREE,
        start_date="2020-01-01",
    )

    def __init__(self):
        super().__init__()
        self.batch_api = BatchAPI()
        self._stream_apis: dict[Environment, StreamAPI] = {}

    def get_stream_api(
        self,
        env: Literal[
            Environment.PAPER,
            Environment.LIVE,
            "PAPER",
            "LIVE",
        ],
    ) -> StreamAPI:
        env = Environment(env.upper())
        if env not in self._stream_apis:
            self._stream_apis[env] = StreamAPI(env=env)
        return self._stream_apis[env]

    def create_product(
        self, basis: str, symbol: str = "", **specs: Any
    ) -> BybitProduct:
        from pfund.venues.bybit.venue import Bybit

        return Bybit.create_product(basis, symbol=symbol, **specs)
