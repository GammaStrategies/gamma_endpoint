"""Token pricing"""

import asyncio
import contextlib

from gql.dsl import DSLQuery

from sources.common.prices.helpers import get_current_prices
from sources.subgraph.bins import LlamaClient
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.subgraphs import SubgraphData
from sources.subgraph.bins.subgraphs.gamma import get_gamma_client


async def gamma_price():
    """Get price of GAMMA"""
    gamma_address = "0x6bea7cfef803d1e3d5f7c0103f7ded065644e197"
    price = (await get_current_prices(Chain.ETHEREUM, [gamma_address]))[0].get(
        "price", 0
    )

    return price


async def token_prices(chain: Chain, protocol: Protocol, session = None) -> dict:
    """Get token prices"""
    token_data = TokenData(chain, protocol)

    await token_data.get_data(session=session)

    # Get prices from defillama and database
    llama_client = LlamaClient(chain)

    llama_prices, db_token_prices = await asyncio.gather(
        llama_client.current_token_price_multi(token_data.data),
        get_current_prices(chain, token_data.data),
    )

    # Base case pricing from DB
    prices = {token["address"]: token["price"] for token in db_token_prices}

    # Find tokens that were not priced in DB
    unpriced_tokens = token_data.data - prices.keys()

    for token in unpriced_tokens:
        prices[token] = llama_prices.get(token, 0)

    with contextlib.suppress(Exception):
        if chain == Chain.POLYGON:
            T_MAINNET = "0xcdf7028ceab81fa0c6971208e83fa7872994bee5"
            T_POLYGON = "0x1d0ab64ed0f1ee4a886462146d26effc6dd00d0b"

            AXL_MAINNET = "0x467719ad09025fcc6cf6f8311755809d45a5e5f3"
            AXL_POLYGON = "0x6e4e624106cb12e168e6533f8ec7c82263358940"

            llama_client_mainnet = LlamaClient(Chain.ETHEREUM)
            prices_mainnet = await llama_client_mainnet.current_token_price_multi(
                [T_MAINNET, AXL_MAINNET]
            )
            prices[T_POLYGON] = prices_mainnet.get(T_MAINNET, 0)
            prices[AXL_POLYGON] = prices_mainnet.get(AXL_MAINNET, 0)

        if chain == Chain.OPTIMISM:
            OP_ADDRESS_OPTIMISM = "0x4200000000000000000000000000000000000042"
            MOCK_OPT_ADDRESS_OPTIMISM = "0x601e471de750cdce1d5a2b8e6e671409c8eb2367"

            prices[MOCK_OPT_ADDRESS_OPTIMISM] = prices.get(OP_ADDRESS_OPTIMISM, 0)

    return prices


class TokenData(SubgraphData):
    """Class to get xGamma staking related data"""

    def __init__(self, chain: Chain, protocol: Protocol):
        super().__init__()
        self.data: {}
        self.client = get_gamma_client(protocol, chain)

    def query(self) -> dict:
        ds = self.client.data_schema

        return DSLQuery(
            ds.Query.uniswapV3Hypervisors(first=1000).select(
                ds.UniswapV3Hypervisor.pool.select(
                    ds.UniswapV3Pool.token0.select(ds.Token.id),
                    ds.UniswapV3Pool.token1.select(ds.Token.id),
                )
            ),
            ds.Query.masterChefV2Rewarders(first=1000).select(
                ds.MasterChefV2Rewarder.rewardToken.select(ds.Token.id)
            ),
        )

    def _transform_data(self) -> dict:
        hype_tokens = []
        for hype in self.query_response["uniswapV3Hypervisors"]:
            hype_tokens.append(hype["pool"]["token0"]["id"])
            hype_tokens.append(hype["pool"]["token1"]["id"])

        mcv2_tokens = [
            rewarder["rewardToken"]["id"]
            for rewarder in self.query_response["masterChefV2Rewarders"]
        ]

        all_tokens = hype_tokens + mcv2_tokens
        all_tokens = list(set(all_tokens))

        return all_tokens
