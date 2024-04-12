from sources.subgraph.bins.config import gamma_subgraph_urls
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.subgraphs import SubgraphClient


class GammaClient(SubgraphClient):
    def __init__(self, protocol: Protocol, chain: Chain):
        super().__init__(
            url=gamma_subgraph_urls[protocol][chain],
            schema_path="sources/subgraph/bins/subgraphs/gamma/schema.graphql",
        )
