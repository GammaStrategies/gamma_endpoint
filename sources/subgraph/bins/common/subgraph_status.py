from pydantic import BaseModel

from sources.subgraph.bins import IndexNodeClient
from sources.subgraph.bins.enums import Chain, Protocol


class SubgraphStatusOutput(BaseModel):
    url: str
    latestBlock: int


async def subgraph_status(protocol: Protocol, chain: Chain) -> SubgraphStatusOutput:
    client = IndexNodeClient(protocol, chain)
    response = await client.status()
    return SubgraphStatusOutput(
        url=response["url"], latestBlock=response["latestBlock"]
    )
