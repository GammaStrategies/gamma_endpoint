from sources.subgraph.bins.config import RECOVERY_POOL_URL
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.subgraphs import SubgraphClient
from sources.subgraph.bins.subgraphs.gamma import get_subgraph_studio_key


class RecoveryPoolClient(SubgraphClient):
    def __init__(self, api_key: str = "prod"):
        super().__init__(
            url=self.studio_url(RECOVERY_POOL_URL, get_subgraph_studio_key(api_key)),
            schema_path="sources/subgraph/bins/subgraphs/recovery_pool/schema.graphql",
        )
