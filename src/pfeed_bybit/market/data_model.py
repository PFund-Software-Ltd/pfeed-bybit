import pfeed as pe
from pfund_blocks.entities.products.product_base import BaseProduct


class BybitMarketDataModel(pe.MarketFeed.DataModel):  # ty: ignore[unsupported-base]
    product: BaseProduct
