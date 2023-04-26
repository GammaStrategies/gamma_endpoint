from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.config import DEX_SUBGRAPH_URLS
from sources.subgraph.bins.subgraphs import SubgraphClient


class UniswapClient(SubgraphClient):
    def __init__(self, protocol: Protocol, chain: Chain):
        super().__init__(
            protocol=protocol,
            chain=chain,
            url=DEX_SUBGRAPH_URLS[protocol][chain],
            schema_path="sources/subgraph/bins/subgraphs/uniswap_v3/schema.graphql",
        )
