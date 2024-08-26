from pydantic import BaseModel

from sources.subgraph.bins.enums import Chain, Protocol


class SubgraphStatusOutput(BaseModel):
    url: str
    latestBlock: int


async def subgraph_status(protocol: Protocol, chain: Chain) -> SubgraphStatusOutput:
    # return SubgraphStatusOutput(
    #     url=response["url"], latestBlock=response["latestBlock"]
    # )
    return SubgraphStatusOutput(url="", latestBlock=0)
