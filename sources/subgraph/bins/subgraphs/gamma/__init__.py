from sources.subgraph.bins.config import (
    SUBGRAPH_STUDIO_KEY,
    SUBGRAPH_STUDIO_USER_KEY,
    gamma_subgraph_ids,
)
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.subgraphs import SubgraphClient


class GammaClient(SubgraphClient):
    def __init__(self, protocol: Protocol, chain: Chain):

        super().__init__(
            subgraph_id=gamma_subgraph_ids[protocol][chain],
            schema_path="sources/subgraph/bins/subgraphs/gamma/schema.graphql",
        )
