from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

import pytest
from pfund_blocks.enums.channel import ChannelType
from pfund_blocks.enums.env import Environment
from pfund_blocks.resolution import Resolution
from pfund_bybit.venue import Bybit as BybitVenue

from pfeed_bybit.source import Bybit
from pfeed_bybit.stream_api import StreamAPI


def create_request(mocker: MockerFixture, basis: str, resolution: str, **specs: Any) -> Any:
    """Mocks a MarketFeedStreamRequest, add_channel() only reads its product and data_resolution."""
    return mocker.Mock(product=Bybit.create_product(basis, **specs), data_resolution=Resolution(resolution))


@pytest.fixture
def stream_api() -> StreamAPI:
    return StreamAPI(env=Environment.LIVE)


class TestAddChannel:
    def test_channel_is_added(self, mocker: MockerFixture, stream_api: StreamAPI):
        stream_api.add_channel(create_request(mocker, "BTC_USDT_PERP", "1m"))
        ws_api = stream_api._ws_apis[BybitVenue.Category.LINEAR]
        assert ws_api.env == Environment.LIVE
        assert ws_api._channels[ChannelType.public] == ["kline.1.BTCUSDT"]

    def test_one_ws_api_per_category(self, mocker: MockerFixture, stream_api: StreamAPI):
        stream_api.add_channel(create_request(mocker, "BTC_USDT_PERP", "1m"))
        stream_api.add_channel(create_request(mocker, "ETH_USDT_PERP", "1t"))
        stream_api.add_channel(create_request(mocker, "BTC_USDT_CRYPTO", "1t"))
        assert set(stream_api._ws_apis) == {BybitVenue.Category.LINEAR, BybitVenue.Category.SPOT}
        linear_ws_api = stream_api._ws_apis[BybitVenue.Category.LINEAR]
        assert linear_ws_api._channels[ChannelType.public] == ["kline.1.BTCUSDT", "publicTrade.ETHUSDT"]

    def test_unsupported_resolution_raises(self, mocker: MockerFixture, stream_api: StreamAPI):
        with pytest.raises(ValueError, match="not supported"):
            stream_api.add_channel(create_request(mocker, "BTC_USDT_PERP", "1s"))


class TestSetCallback:
    def test_ws_apis_parse_messages(self, mocker: MockerFixture, stream_api: StreamAPI):
        """Without Ray, the websocket APIs parse the messages, so that the user callback receives the parsed data."""
        stream_api.add_channel(create_request(mocker, "BTC_USDT_PERP", "1m"))
        stream_api.add_channel(create_request(mocker, "BTC_USDT_CRYPTO", "1t"))
        callback = mocker.Mock()
        stream_api.set_callback(callback)
        assert stream_api._callback is callback
        for ws_api in stream_api._ws_apis.values():
            assert ws_api._callback is callback
            assert ws_api._skip_parse_message is False
            assert ws_api._include_raw_message is True

    def test_ws_apis_skip_parsing(self, mocker: MockerFixture, stream_api: StreamAPI):
        """With Ray, the workers parse the messages, so the websocket APIs must not parse them but keep the raw message."""
        stream_api.add_channel(create_request(mocker, "BTC_USDT_PERP", "1m"))
        stream_api.add_channel(create_request(mocker, "BTC_USDT_CRYPTO", "1t"))
        stream_api.set_callback(mocker.Mock(), skip_parse_message=True)
        for ws_api in stream_api._ws_apis.values():
            assert ws_api._skip_parse_message is True
            assert ws_api._include_raw_message is True
