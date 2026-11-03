from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfund_blocks.typing import Message
    from pytest_mock import MockerFixture

from pfund_blocks.datas.bar_data import Bar
from pfund_blocks.datas.tick_data import Tick
from pfund_blocks.enums import Side
from pfund_blocks.resolution import Resolution

from pfeed_bybit.market.feed import BybitMarketFeed


def create_message(channel: str, decoded_message: dict) -> Message:
    """An unparsed message as the websocket API sends it with skip_parse_message=True."""
    return {
        "source": "BYBIT",
        "account": None,
        "channel": channel,
        "product": "BYBIT_BTC_USDT_PERP",
        "resolution": "1m",
        "received_at": 1793707610700000000,
        "message": decoded_message,
        "data": None,
        "error": None,
    }


class TestParseStreamingMessage:
    def test_candlestick(self):
        decoded_message = {
            "topic": "kline.1.BTCUSDT",
            "type": "snapshot",
            "ts": 1793707610671,
            "data": [
                {
                    "start": 1793707560000,
                    "end": 1793707619999,
                    "interval": "1",
                    "open": "77047.7",
                    "close": "77026.6",
                    "high": "77049.9",
                    "low": "77010.1",
                    "volume": "4.949",
                    "turnover": "381238.9483",
                    "confirm": False,
                    "timestamp": 1793707610671,
                }
            ],
        }
        msg = create_message("kline.1.BTCUSDT", decoded_message)
        parsed = BybitMarketFeed._parse_streaming_message(msg)
        [bar] = parsed["data"]
        assert isinstance(bar, Bar)
        assert bar.resolution == Resolution("1m")
        assert bar.close == 77026.6
        assert bar.server_ts == 1793707610671 * 1_000_000
        # everything but data is kept
        assert {k: v for k, v in parsed.items() if k != "data"} == {k: v for k, v in msg.items() if k != "data"}

    def test_tradebook(self):
        decoded_message = {
            "topic": "publicTrade.BTCUSDT",
            "type": "snapshot",
            "ts": 1793708150597,
            "data": [
                {"T": 1793708150596, "s": "BTCUSDT", "S": "Buy", "v": "0.131", "p": "77220.00"},
                {"T": 1793708150596, "s": "BTCUSDT", "S": "Sell", "v": "0.002", "p": "77219.90"},
            ],
        }
        msg = create_message("publicTrade.BTCUSDT", decoded_message)
        ticks = BybitMarketFeed._parse_streaming_message(msg)["data"]
        assert all(isinstance(tick, Tick) for tick in ticks)
        assert [(tick.price, tick.volume, tick.side) for tick in ticks] == [
            (77220.0, 0.131, Side.BUY),
            (77219.9, 0.002, Side.SELL),
        ]

    def test_input_message_is_not_mutated(self):
        decoded_message = {
            "topic": "publicTrade.BTCUSDT",
            "type": "snapshot",
            "ts": 1793708150597,
            "data": [{"T": 1793708150596, "s": "BTCUSDT", "S": "Buy", "v": "0.131", "p": "77220.00"}],
        }
        msg = create_message("publicTrade.BTCUSDT", decoded_message)
        BybitMarketFeed._parse_streaming_message(msg)
        assert msg["data"] is None

    def test_parsed_message_is_not_parsed_again(self, mocker: MockerFixture):
        from pfund_bybit.ws_api import BybitWebSocketAPI

        parse_message = mocker.spy(BybitWebSocketAPI.parser, "parse_message")
        msg = create_message("publicTrade.BTCUSDT", {"topic": "publicTrade.BTCUSDT", "ts": 1793708150597, "data": []})
        msg["data"] = []
        assert BybitMarketFeed._parse_streaming_message(msg) is msg
        parse_message.assert_not_called()
