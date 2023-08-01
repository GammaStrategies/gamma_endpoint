from sources.common.general.enums import Chain, Dex, Protocol

from ..bins.w3.builders import (
    build_hypervisor,
    build_hypervisor_registry,
)
from ..bins.w3.protocols.general import web3wrap
from ..bins.mixed.price_utilities import price_scraper


async def hypervisors_list(chain: Chain, protocol: Protocol):
    # get network registry address
    registry = build_hypervisor_registry(
        network=chain.database_name,
        protocol=protocol,
        block=0,
    )

    return registry.get_hypervisors_addresses()


async def hypervisor_uncollected_fees(
    chain: Chain, protocol: Protocol, hypervisor_address: str, block: int = None
):
    if hypervisor := build_hypervisor(
        network=chain.database_name,
        protocol=protocol,
        block=block or 0,
        hypervisor_address=hypervisor_address,
    ):
        return hypervisor.get_fees_uncollected()


async def get_hypervisor_data(
    chain: Chain,
    protocol: Dex,
    hypervisor_address: str,
    fields: list[str],
    block: int | None = None,
):
    if hype := build_hypervisor(
        network=chain.database_name,
        protocol=protocol,
        block=block or 0,
        hypervisor_address=hypervisor_address,
    ):
        pass
