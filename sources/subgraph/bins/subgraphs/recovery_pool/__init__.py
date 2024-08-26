from sources.subgraph.bins.config import RECOVERY_POOL
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.subgraphs import SubgraphClient


class RecoveryPoolClient(SubgraphClient):
    """Subgraph client for Recovery Pool subgraph"""
    def __init__(self):
        super().__init__(
            subgraph_id=RECOVERY_POOL,
            schema_path="sources/subgraph/bins/subgraphs/recovery_pool/schema.graphql",
        )
