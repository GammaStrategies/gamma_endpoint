import random
from sources.common.general.enums import Chain, Dex

from sources.web3.bins.w3.objects.protocols import (
    gamma_hypervisor,
    gamma_hypervisor_ramses,
    gamma_hypervisor_zyberswap,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_thena,
    gamma_hypervisor_registry,
)
from sources.web3.bins.w3.objects.rewarders import (
    thena_gauge_V2,
    thena_voter_v3,
    zyberswap_masterchef_v1,
)
from sources.web3.bins.w3.objects.basic import erc20
from sources.web3.bins.configuration import STATIC_REGISTRY_ADDRESSES


def build_hypervisor(
    network: Chain,
    dex: Dex,
    block: int,
    hypervisor_address: str,
    custom_web3Url: str | None = None,
) -> gamma_hypervisor:
    # choose type based on dex
    if dex == Dex.ZYBERSWAP:
        hypervisor = gamma_hypervisor_zyberswap(
            address=hypervisor_address,
            network=network.database_name,
            block=block,
            custom_web3Url=custom_web3Url,
        )
    elif dex == Dex.QUICKSWAP:
        hypervisor = gamma_hypervisor_quickswap(
            address=hypervisor_address,
            network=network.database_name,
            block=block,
            custom_web3Url=custom_web3Url,
        )
    elif dex == Dex.THENA:
        hypervisor = gamma_hypervisor_thena(
            address=hypervisor_address,
            network=network.database_name,
            block=block,
            custom_web3Url=custom_web3Url,
        )
    elif dex == Dex.RAMSES:
        hypervisor = gamma_hypervisor_ramses(
            address=hypervisor_address,
            network=network.database_name,
            block=block,
            custom_web3Url=custom_web3Url,
        )
    else:
        # build hype
        hypervisor = gamma_hypervisor(
            address=hypervisor_address,
            network=network.database_name,
            block=block,
            custom_web3Url=custom_web3Url,
        )

    return hypervisor


def build_hypervisor_registry(
    network: Chain,
    dex: Dex,
    block: int,
    custom_web3Url: str | None = None,
) -> gamma_hypervisor_registry:
    # get the list of registry addresses

    if registry_address := (
        STATIC_REGISTRY_ADDRESSES.get(network.database_name, {})
        .get("hypervisors", {})
        .get(dex.value)
    ):
        # build hype
        registry = gamma_hypervisor_registry(
            address=registry_address,
            network=network.database_name,
            block=block,
            custom_web3Url=custom_web3Url,
        )

        return registry


def build_erc20_helper(chain: Chain, address: str | None = None) -> erc20:
    """Create a erc20 with the zero address

    Args:
        chain (Chain):
        cached (bool, optional): . Defaults to False.

    Returns:
        bep20 | erc20:
    """

    return erc20(
        address=address or "0x0000000000000000000000000000000000000000",
        network=chain.database_name,
    )
