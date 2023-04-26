from sources.common.general.enums import Chain, Dex, ChainId

from sources.web3.bins.w3.objects.protocols import (
    gamma_hypervisor,
    gamma_hypervisor_zyberswap,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_thena,
    gamma_hypervisor_registry,
)

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

    if registry_address := (
        STATIC_REGISTRY_ADDRESSES.get(network.value, {})
        .get("hypervisors", {})
        .get(dex.value)
    ):
        # build hype
        registry = gamma_hypervisor_registry(
            address=registry_address,
            network=network.value,
            block=block,
            custom_web3Url=custom_web3Url,
        )

        return registry


def build_hypervisor_anyRpc(
    network: Chain, dex: Dex, block: int, hypervisor_address: str, rpcUrls: list[str]
) -> gamma_hypervisor:
    """return a tested hype that uses any of the supplyed RPC urls

    Args:
        network (str):
        dex (str):
        block (int):
        hypervisor_address (str):

    Returns:
        gamma_hypervisor:
    """
    for rpcUrl in rpcUrls:
        try:
            # construct hype
            hypervisor = build_hypervisor(
                network=network,
                dex=dex,
                block=block,
                hypervisor_address=hypervisor_address,
                rpcUrl=rpcUrl,
            )
            # test its working
            hypervisor.fee
            # return hype
            return hypervisor
        except:
            # not working hype
            pass

    return None


def build_hypervisor_registry_anyRpc(
    network: Chain, dex: Dex, block: int, rpcUrls: list[str]
) -> gamma_hypervisor_registry:
    """return a tested hype registry that uses any of the supplyed RPC urls

    Args:
        network (str):
        dex (str):
        block (int):

    Returns:
        gamma hype registry:
    """
    for rpcUrl in rpcUrls:
        try:
            # construct hype
            registry = build_hypervisor_registry(
                network=network,
                dex=dex,
                block=block,
                custom_web3Url=rpcUrl,
            )
            # test its working
            registry.counter
            # return hype
            return registry
        except:
            # not working hype
            pass

    return None
