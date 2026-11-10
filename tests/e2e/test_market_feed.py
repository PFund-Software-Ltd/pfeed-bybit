from __future__ import annotations

import pytest
from pfeed.testing import FeedCase, MarketFeedE2ETests

from pfeed_bybit import Bybit

pytestmark = pytest.mark.network


class TestBybitMarketFeed(MarketFeedE2ETests):
    source = Bybit
    start_date, end_date = '2026-09-01', '2026-09-02'
    cases = [
        FeedCase('BTC_USDT_PERP', '1t'),  # USDT perpetual
        FeedCase('BTC_USDT_FUT', '1s', {'expiration': '2026-09-25'}),  # USDT future
        FeedCase('BTC_USDC_PERPETUAL', '1m'),  # USDC perpetual
        # DEPRECATED: FeedCase('BTC_USDC_FUTURE', '1h', {'expiration': '2026-09-25'}),  # USDC futures NO LONGER EXIST
        FeedCase('BTC_USD_IPERP', '1d'),  # inverse perpetual
        FeedCase('BTC_USD_INVERSE-FUTURE', '1t', {'expiration': '2026-09-25'}),  # inverse future
        FeedCase('BTC_USDC_CRYPTO', '1t'),  # spot
    ]
    # BTCUSDT perpetual wasn't listed yet, Bybit has no files for these dates and must answer 404,
    # which BatchAPI turns into no data; any other status (e.g. 403) raises an HTTP error instead
    missing_dates_case = FeedCase('BTC_USDT_PERP', '1t', start_date='2020-01-01', end_date='2020-01-02')


# TODO
# @pytest.mark.parametrize(('product', 'resolution'), [
#     ('HYPE_USDT_PERP', '1q_L2'),
#     ('XRP_USDT_CRYPTO', '10q_L1'),
#     ('SOL_USD_IPERP', '1m'),
# ])
# def test_stream_and_retrieve(tmp_path, bybit, product, resolution):
#     def _callback(msg):
#         print(msg)
#     bybit.stream(product=product, resolution=resolution, callback=_callback)


# TODO
# @pytest.mark.parametrize(('product', 'resolution'), [('BTC_USDT_PERP', '1q_L2')])
# @pytest.mark.asyncio
# async def test_stream_and_retrieve_async(tmp_path, bybit, product, resolution):
#     async for msg in bybit.stream(product=product, resolution=resolution):
#         print(msg)
