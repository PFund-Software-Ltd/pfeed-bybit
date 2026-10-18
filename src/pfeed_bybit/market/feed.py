from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, cast

if TYPE_CHECKING:
    from pfeed.market.requests.base_request import MarketFeedBaseRequest

    from pfeed_bybit.source import Bybit

import pfeed as pe
import polars as pl

from pfeed_bybit.market.data_model import BybitMarketDataModel


# TODO: add back StreamingFeedMixin
class BybitMarketFeed(pe.MarketFeed["Bybit", "MarketFeedBaseRequest", BybitMarketDataModel]):
    DataModel: ClassVar[type[BybitMarketDataModel]] = BybitMarketDataModel  # ty: ignore[invalid-mutable-override]

    class Requests(pe.MarketFeed.Requests):  # ty: ignore[invalid-attribute-override]
        # TODO: re-enable stream capability
        stream = None

    downloaded_data_date_cols: ClassVar[list[str]] = ["timestamp"]
    # Bybit's batch API serves one file per product per day
    download_dataflow_per_date: ClassVar[bool] = True

    @staticmethod
    def _normalize_downloaded_data(df: pl.LazyFrame) -> pl.LazyFrame:
        """Normalize raw Bybit DataFrame into a consistent format.

        Args:
            df: DataFrame after `_standardize_date_column`

        Returns:
            Normalized DataFrame with:
            - 'size' renamed to 'volume'
            - 'side' mapped from Buy/Sell (case-insensitive) to 1/-1
            - volume cast to float64
        """
        # Bybit spot CSVs ship 'volume' directly (and an 'rpi' column);
        # non-spot still ships 'size'. strict=False tolerates either schema.
        RENAMING_COLS: dict[str, str] = {"size": "volume"}
        MAPPING_COLS: dict[str, int] = {"buy": 1, "sell": -1}
        return df.rename(RENAMING_COLS, strict=False).with_columns(
            # some products use "Buy"/"Sell" while some use "buy"/"sell";
            # replace_strict errors on unexpected values rather than silently producing nulls
            pl.col("side")
            .str.to_lowercase()
            .replace_strict(MAPPING_COLS, return_dtype=pl.Int8),
            # cast to float64 to pass pandera schema validation — inverse products have int volume
            pl.col("volume").cast(pl.Float64),
        )

    @staticmethod
    def _download_impl(
        request: MarketFeedBaseRequest, data_model: BybitMarketDataModel
    ) -> pl.LazyFrame | None:
        data_source = cast("Bybit", request.data_source)
        assert data_model.start_date == data_model.end_date, (
            f"{data_source.name} download() only supports downloading data for a single day"
        )
        # data_model.resolution is the target resolution, download the raw data at data_resolution
        return data_source.get_batch_api().get_data(
            product=data_model.product,
            resolution=request.data_resolution,
            date=data_model.start_date,
        )


#     def _get_stream_api(self, env: Environment) -> StreamAPI:
#         return self.data_source.get_stream_api(env)
#
#     @staticmethod
#     def _parse_streaming_message(product: BybitProduct, msg: RawMessage) -> ResponseData:
#         from pfund.venues.bybit._ws_apis.ws_api_base import BybitBaseWebSocketAPI
#         from pfund.venues.bybit.ws_api import BybitWebSocketAPI
#
#         assert product.category is not None, "product.category is not initialized"
#         WebSocketAPI: type[BybitBaseWebSocketAPI] = BybitWebSocketAPI.APIS[
#             product.category
#         ]
#
#         channel: str = msg["topic"]
#         if channel.startswith("kline"):
#             return WebSocketAPI._parse_candlestick(msg)
#         elif channel.startswith("publicTrade"):
#             return WebSocketAPI._parse_tradebook(msg)
#         # TODO: handle orderbook
#         # elif channel.startswith('orderbook'):
#         #     return WebSocketAPI._parse_orderbook(msg)
#         else:
#             raise NotImplementedError(
#                 f"{WebSocketAPI.venue} {product.category} {channel=} is not supported"
#             )
#
#     @staticmethod
#     def _normalize_streaming_timestamp(msg: ResponseData) -> ResponseData:
#         """Bybit timestamps are in milliseconds, convert to nanoseconds"""
#         msg["ts"] = int(msg["ts"] * 10**6)
#         data = msg["data"]
#         if "ts" in data:
#             data["ts"] = int(data["ts"] * 10**6)
#         if "start_ts" in data:
#             data["start_ts"] = int(data["start_ts"] * 10**6)
#         if "end_ts" in data:
#             data["end_ts"] = int(data["end_ts"] * 10**6)
#         return msg
