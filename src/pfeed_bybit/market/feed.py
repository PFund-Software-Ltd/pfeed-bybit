from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

if TYPE_CHECKING:
    from pfeed.market.requests.base_request import MarketFeedBaseRequest
    from pfund_blocks.typing import Message

    from pfeed_bybit.source import Bybit

import pfeed as pe
import polars as pl
from pfeed.market.data_model import MarketDataModel
from pfeed.market.requests.stream_request import MarketFeedStreamRequest
from pfeed.streaming.market import MarketFeedStreamingMixin


class BybitMarketFeed[PipelineT: bool = Literal[False]](
    MarketFeedStreamingMixin[MarketFeedStreamRequest, MarketDataModel],
    pe.MarketFeed["Bybit[PipelineT]", "MarketFeedBaseRequest", MarketDataModel, PipelineT]
):
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
        request: MarketFeedBaseRequest, data_model: MarketDataModel
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

    @staticmethod
    def _parse_streaming_message(msg: Message) -> Message:
        from pfund_bybit.ws_api import BybitWebSocketAPI

        # NOTE: already parsed by the websocket API, only unparsed with Ray, where parsing is left to the workers
        is_parsed_already = msg['data'] is not None
        if is_parsed_already:
            return msg
        channel = cast(str, msg['channel'])
        message = cast(dict[str, Any], msg['message'])
        return {**msg, 'data': BybitWebSocketAPI.parser.parse_message(channel, message)}
