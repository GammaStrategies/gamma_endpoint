from sources.common.general.enums import Chain, Dex, ChainId

# from sources.web3.bins.w3.objects.protocols import gamma_hypervisor_registry
from sources.web3.bins.w3.helpers import (
    build_hypervisor,
    build_hypervisor_anyRpc,
    build_hypervisor_registry,
    build_hypervisor_registry_anyRpc,
)

from sources.web3.bins.configuration import RPC_URLS, CONFIGURATION


def hypervisors_list(network: Chain, dex: Dex):
    # get network registry address
    registry = build_hypervisor_registry_anyRpc(
        network=network, dex=dex, block=0, rpcUrls=RPC_URLS[network.value]
    )

    return registry.get_hypervisors_addresses()
