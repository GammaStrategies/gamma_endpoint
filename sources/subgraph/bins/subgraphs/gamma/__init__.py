from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.config import GAMMA_SUBGRAPH_URLS
from sources.subgraph.bins.subgraphs import SubgraphClient


class GammaClient(SubgraphClient):
    def __init__(self, protocol: Protocol, chain: Chain):
        super().__init__(
            protocol=protocol,
            chain=chain,
            url=GAMMA_SUBGRAPH_URLS[protocol][chain],
            schema_path="sources/subgraph/bins/subgraphs/gamma/schema.graphql",
        )
