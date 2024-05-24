from sources.subgraph.bins.config import (
    SUBGRAPH_STUDIO_KEY,
    SUBGRAPH_STUDIO_KEY_DEV,
    SUBGRAPH_STUDIO_USER_KEY,
    gamma_subgraph_urls,
)
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.subgraphs import SubgraphClient


def get_subgraph_studio_key(api_key: str) -> str:
    if api_key == "dev":
        key = SUBGRAPH_STUDIO_KEY_DEV
    elif api_key == "gamma_users":
        key = SUBGRAPH_STUDIO_USER_KEY
    else:
        key = SUBGRAPH_STUDIO_KEY

    return key


class GammaClient(SubgraphClient):
    def __init__(self, protocol: Protocol, chain: Chain, api_key: str = "prod"):

        super().__init__(
            url=self.studio_url(
                gamma_subgraph_urls[protocol][chain], get_subgraph_studio_key(api_key)
            ),
            schema_path="sources/subgraph/bins/subgraphs/gamma/schema.graphql",
        )
