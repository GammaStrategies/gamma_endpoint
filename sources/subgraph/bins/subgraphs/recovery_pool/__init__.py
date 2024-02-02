from sources.subgraph.bins.config import RECOVERY_POOL_URL
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.subgraphs import SubgraphClient


class RecoveryPoolClient(SubgraphClient):
    def __init__(self):
        super().__init__(
            url=RECOVERY_POOL_URL,
            schema_path="sources/subgraph/bins/subgraphs/recovery_pool/schema.graphql",
        )
