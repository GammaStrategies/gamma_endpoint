from fastapi_cache.decorator import cache

from sources.subgraph.bins.charts.base_range import BaseLimit
from sources.subgraph.bins.config import CHARTS_CACHE_TIMEOUT
from sources.subgraph.bins.enums import Chain, Protocol


@cache(expire=CHARTS_CACHE_TIMEOUT)
async def base_range_chart_all(protocol: Protocol, chain: Chain, days: int = 20):
    hours = days * 24
    baseLimitData = BaseLimit(protocol=protocol, hours=hours, chart=True, chain=chain)
    chart_data = await baseLimitData.all_rebalance_ranges()
    return chart_data


@cache(expire=CHARTS_CACHE_TIMEOUT)
async def base_range_chart(
    protocol: Protocol, chain: Chain, hypervisor_address: str, days: int = 20
):
    hours = days * 24
    hypervisor_address = hypervisor_address.lower()
    baseLimitData = BaseLimit(protocol=protocol, hours=hours, chart=True, chain=chain)
    chart_data = await baseLimitData.rebalance_ranges(hypervisor_address)
    return {hypervisor_address: chart_data} if chart_data else {}
