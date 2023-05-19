import random
from sources.common.general.enums import Chain, Dex, ChainId
from sources.mongo.bins.enums import enumsConverter

from sources.web3.bins.w3.objects.protocols import (
    gamma_hypervisor,
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
            network=network.value,
            block=block,
            custom_web3Url=custom_web3Url,
        )
    elif dex == Dex.QUICKSWAP:
        hypervisor = gamma_hypervisor_quickswap(
            address=hypervisor_address,
            network=network.value,
            block=block,
            custom_web3Url=custom_web3Url,
        )
    elif dex == Dex.THENA:
        hypervisor = gamma_hypervisor_thena(
            address=hypervisor_address,
            network=network.value,
            block=block,
            custom_web3Url=custom_web3Url,
        )
    else:
        # build hype
        hypervisor = gamma_hypervisor(
            address=hypervisor_address,
            network=network.value,
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

    netval = enumsConverter.convert_general_to_local(chain=network).value

    if registry_address := (
        STATIC_REGISTRY_ADDRESSES.get(netval, {}).get("hypervisors", {}).get(dex.value)
    ):
        # build hype
        registry = gamma_hypervisor_registry(
            address=registry_address,
            network=network.value,
            block=block,
            custom_web3Url=custom_web3Url,
        )

        return registry
