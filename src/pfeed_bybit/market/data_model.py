import pfeed as pe
from pfund.venues.bybit.product import BybitProduct


class BybitMarketDataModel(pe.MarketFeed.DataModel):  # ty: ignore[unsupported-base]
    product: BybitProduct
