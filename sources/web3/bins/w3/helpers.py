import random
from sources.common.general.enums import Chain, Dex, ChainId

from sources.web3.bins.w3.objects.protocols import (
    gamma_hypervisor,
    gamma_hypervisor_zyberswap,
    gamma_hypervisor_quickswap,
    gamma_hypervisor_thena,
    gamma_hypervisor_registry,
    zyberswap_masterchef_v1,
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


async def build_hypervisor_anyRpc(
    network: Chain,
    dex: Dex,
    block: int,
    hypervisor_address: str,
    rpcUrls: list[str],
    test: bool = False,
) -> gamma_hypervisor:
    """return a tested hype that uses any of the supplyed RPC urls

    Args:
        network (str):
        dex (str):
        block (int):
        hypervisor_address (str):
        rpcUrls (list[str]): list of RPC urls to be used
        test: (bool): if true, test the hype before returning it

    Returns:
        gamma_hypervisor:
    """
    # shuffle the rpc urls
    random.shuffle(rpcUrls)
    # loop over the rpc urls
    hypervisor = None
    for rpcUrl in rpcUrls:
        try:
            # construct hype
            hypervisor = build_hypervisor(
                network=network,
                dex=dex,
                block=block,
                hypervisor_address=hypervisor_address,
                custom_web3Url=rpcUrl,
            )
            if test:
                # working test
                await hypervisor._contract.functions.fee().call()  # test fee without block
            # return hype
            break
        except Exception as e:
            # not working hype
            print(f" error creating hype: {e} -> rpc: {rpcUrl}")
    # return hype
    return hypervisor


async def build_hypervisor_registry_anyRpc(
    network: Chain, dex: Dex, block: int, rpcUrls: list[str], test: bool = False
) -> gamma_hypervisor_registry:
    """return a hype registry that uses any of the supplyed RPC urls

    Args:
        network (str):
        dex (str):
        block (int):
        test: (bool): if true, test the hype before returning it

    Returns:
        gamma hype registry:
    """
    # shuffle the rpc urls
    random.shuffle(rpcUrls)
    # loop over the rpc urls
    registry = None
    for rpcUrl in rpcUrls:
        try:
            # construct hype
            registry = build_hypervisor_registry(
                network=network,
                dex=dex,
                block=block,
                custom_web3Url=rpcUrl,
            )
            if test:
                # test its working
                await registry._contract.functions.counter().call()
            # return hype
            break
        except:
            # not working hype
            pass

    return registry


async def build_zyberchef_anyRpc(
    address: str, network: Chain, block: int, rpcUrls: list[str], test: bool = False
) -> zyberswap_masterchef_v1:
    """return a hype registry that uses any of the supplyed RPC urls

    Args:
        network (str):
        block (int):
        test: (bool): if true, test the hype before returning it

    """
    # shuffle the rpc urls
    random.shuffle(rpcUrls)
    # loop over the rpc urls
    result = None
    for rpcUrl in rpcUrls:
        try:
            # construct hype
            result = zyberswap_masterchef_v1(
                address=address,
                network=network,
                block=block,
                custom_web3Url=rpcUrl,
            )
            if test:
                # test its working
                # await result.poolLength
                pass
            # return hype
            break
        except:
            # not working hype
            pass

    return result
