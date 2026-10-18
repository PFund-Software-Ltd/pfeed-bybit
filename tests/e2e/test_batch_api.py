from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pfeed as pe

import pytest
from pfund.datas.resolution import Resolution

pytestmark = pytest.mark.network

TRADING_COLUMNS = [
    'timestamp', 'symbol', 'side', 'size', 'price', 'tickDirection', 'trdMatchID',
    'grossValue', 'homeNotional', 'foreignNotional', 'RPI',
]
SPOT_COLUMNS = ['id', 'timestamp', 'price', 'volume', 'side', 'rpi']


# detects schema drift in Bybit's raw files, i.e. before any normalization by pfeed-bybit or pfeed;
# a failure means Bybit changed its file format, update the expected columns and check _normalize_downloaded_data
@pytest.mark.parametrize(('product', 'product_specs', 'expected_columns'), [
    ('BTC_USDT_PERP', {}, TRADING_COLUMNS),
    ('BTC_USDT_FUT', {'expiration': '2026-09-25'}, TRADING_COLUMNS),
    ('BTC_USD_IPERP', {}, TRADING_COLUMNS),
    ('BTC_USD_INVERSE-FUTURE', {'expiration': '2026-09-25'}, TRADING_COLUMNS),
    ('BTC_USDC_SPOT', {}, SPOT_COLUMNS),
])
def test_raw_columns(bybit: pe.Bybit, product: str, product_specs: dict[str, Any], expected_columns: list[str]):
    data = bybit.get_batch_api().get_data(
        product=bybit.create_product(product, **product_specs),
        resolution=Resolution('1t'),
        date='2026-09-01',
    )
    assert data is not None
    assert data.collect_schema().names() == expected_columns
