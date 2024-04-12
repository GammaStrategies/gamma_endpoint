from sources.subgraph.bins.config import dex_subgraph_urls
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.subgraphs import SubgraphClient


class UniswapClient(SubgraphClient):
    def __init__(self, protocol: Protocol, chain: Chain):
        super().__init__(
            url=dex_subgraph_urls[protocol][chain],
            schema_path="sources/subgraph/bins/subgraphs/uniswap_v3/schema.graphql",
        )
