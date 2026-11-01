from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pfund_blocks.entities.products.product_base import BaseProduct
    from pfund_blocks.enums.env import Environment
    from pfund_blocks.resolution import Resolution
    from pfund_blocks.typing import Message

import asyncio
import logging

from pfeed.streaming.stream_api import BaseStreamAPI
from pfund_bybit.venue import Bybit
from pfund_bybit.ws_api import BybitWebSocketAPI


class StreamAPI(BaseStreamAPI):
    """Simple wrapper of exchange's websocket APIs to connect to public channels,
    one websocket API per category, since Bybit has one public endpoint per category.
    """

    def __init__(self, env: Literal[Environment.PAPER, Environment.LIVE]):
        super().__init__(env)
        self._logger = logging.getLogger("pfeed.bybit")
        self._ws_apis: dict[Bybit.Category, BybitWebSocketAPI] = {}

    def _add_ws_api(self, product: BaseProduct) -> BybitWebSocketAPI:
        category = Bybit.get_category(product)
        # NOTE: only categories with channels have a websocket API, since connect() raises if there are no channels
        if category not in self._ws_apis:
            ws_api = BybitWebSocketAPI(env=self._env, category=category)
            ws_api._logger = self._logger
            self._ws_apis[category] = ws_api
        return self._ws_apis[category]

    def get_supported_resolutions(self, product: BaseProduct) -> list[Resolution]:
        return BybitWebSocketAPI.get_supported_resolutions(product)

    def add_channel(self, product: BaseProduct, resolution: Resolution) -> None:
        ws_api = self._add_ws_api(product)
        channel = ws_api.create_market_data_channel(product, resolution)
        ws_api.add_channel(channel, channel_type="public")

    def set_callback(
        self,
        callback: Callable[[Message], Awaitable[None] | None],
        *,
        include_raw_message: bool = True,
    ) -> None:
        super().set_callback(callback)
        for ws_api in self._ws_apis.values():
            ws_api.set_callback(callback, include_raw_message=include_raw_message)

    async def connect(self) -> None:
        for ws_api in self._ws_apis.values():
            await ws_api.connect()

    async def wait_until_disconnected(self) -> None:
        await asyncio.gather(*[ws_api.wait_until_disconnected() for ws_api in self._ws_apis.values()])

    async def disconnect(self) -> None:
        await asyncio.gather(*[ws_api.disconnect() for ws_api in self._ws_apis.values()])
