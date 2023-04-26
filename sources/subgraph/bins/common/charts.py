from fastapi_cache.decorator import cache

from sources.subgraph.bins.charts.base_range import BaseLimit
from sources.subgraph.bins.charts.benchmark import Benchmark
from sources.subgraph.bins.config import CHARTS_CACHE_TIMEOUT
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.utils import parse_date


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
    if chart_data:
        return {hypervisor_address: chart_data}
    else:
        return {}


async def benchmark_chart(
    protocol: Protocol,
    chain: Chain,
    hypervisor_address: str,
    startDate: str = "",
    endDate: str = "",
):
    start_date = parse_date(startDate)
    end_date = parse_date(endDate)
    hypervisor_address = hypervisor_address.lower()
    benchmark = Benchmark(protocol, chain, hypervisor_address, start_date, end_date)
    chart_data = await benchmark.chart()
    if chart_data:
        return {hypervisor_address: chart_data}
    else:
        return {}
