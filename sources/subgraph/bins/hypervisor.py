import asyncio
import logging

from sources.subgraph.bins import GammaClient
from sources.subgraph.bins.config import EXCLUDED_HYPERVISORS
from sources.subgraph.bins.data import BlockRange
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.utils import filter_address_by_chain

logger = logging.getLogger(__name__)


class HypervisorData:
    def __init__(self, protocol: Protocol, chain: Chain = Chain.ETHEREUM):
        self.protocol = protocol
        self.chain = chain
        self.gamma_client = GammaClient(protocol, chain)
        self.basics_data = {}
        self.pools_data = {}
        self.fees_data = {}

        self.excluded_hypervisors = filter_address_by_chain(EXCLUDED_HYPERVISORS, chain)

    async def _get_hypervisor_data(self, hypervisor_address):
        query = """
        query hypervisor($id: String!){
            uniswapV3Hypervisor(
                id: $id
            ) {
                id
                created
                baseLower
                baseUpper
                totalSupply
                maxTotalSupply
                deposit0Max
                deposit1Max
                grossFeesClaimed0
                grossFeesClaimed1
                grossFeesClaimedUSD
                feesReinvested0
                feesReinvested1
                feesReinvestedUSD
                tvl0
                tvl1
                tvlUSD
                pool{
                    id
                    fee
                    token0{
                        symbol
                        decimals
                    }
                    token1{
                        symbol
                        decimals
                    }
                    sqrtPriceX96
                }
            }
        }
        """
        variables = {"id": hypervisor_address.lower()}
        response = await self.gamma_client.query(query, variables)

        # TODO: hardcoded hypervisor address matches more than one -->
        #       MAINNET(xPSDN-ETH1) and OPTIMISM(xUSDC-DAI05)
        # TODO: specify chain on hardcoded overrides
        if hypervisor_address == "0x0ec4a47065bf52e1874d2491d4deeed3c638c75f":
            response["data"]["uniswapV3Hypervisor"]["grossFeesClaimedUSD"] = str(
                float(response["data"]["uniswapV3Hypervisor"]["grossFeesClaimedUSD"])
                - 238300
            )
            response["data"]["uniswapV3Hypervisor"]["feesReinvestedUSD"] = str(
                float(response["data"]["uniswapV3Hypervisor"]["feesReinvestedUSD"])
                - 214470
            )

        return response["data"]["uniswapV3Hypervisor"]

    async def _get_all_data(self):
        query_basics = """
        {
            uniswapV3Hypervisors(
                first:1000
            ){
                id
                created
                baseLower
                baseUpper
                totalSupply
                maxTotalSupply
                deposit0Max
                deposit1Max
                grossFeesClaimed0
                grossFeesClaimed1
                grossFeesClaimedUSD
                feesReinvested0
                feesReinvested1
                feesReinvestedUSD
                tvl0
                tvl1
                tvlUSD
                pool{
                    id
                    fee
                    token0{
                        id
                        symbol
                        decimals
                    }
                    token1{
                        id
                        symbol
                        decimals
                    }
                    sqrtPriceX96
                }
            }
        }
        """

        basics_response = await self.gamma_client.query(query_basics)

        # TODO: hardcoded hypervisor address matches more than one -->
        #       MAINNET(xPSDN-ETH1) and OPTIMISM(xUSDC-DAI05)
        # TODO: specify chain on hardcoded overrides
        for hypervisor in basics_response["data"]["uniswapV3Hypervisors"]:
            if hypervisor["id"] == "0x0ec4a47065bf52e1874d2491d4deeed3c638c75f":
                hypervisor["grossFeesClaimedUSD"] = str(
                    float(hypervisor["grossFeesClaimedUSD"]) - 238300
                )
                hypervisor["feesReinvestedUSD"] = str(
                    float(hypervisor["feesReinvestedUSD"]) - 214470
                )

        basics = basics_response["data"]["uniswapV3Hypervisors"]

        self.basics_data = basics
        self.pools_data = {}

    async def _get_collected_fees(
        self,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ) -> dict:
        """Get collected fees for all hypervisors in a given month and year, from the subgraph.


        Args:
            start_timestamp (int): start timestamp of the time range
            end_timestamp (int): end timestamp of the time range

        Returns:
            dict: dictionary with hypervisor address as key and collected fee related fields as value
        """

        if (
            not start_timestamp
            and not start_block
            or not end_timestamp
            and not end_block
        ):
            raise ValueError("timestamp or block must be provided")

        time_range = BlockRange(chain=self.chain, subgraph_client=self.gamma_client)

        if not start_block or not end_block:
            # overwrite any provided block with timestamp related
            # await time_range.set_initial_with_timestamp(timestamp=start_timestamp)
            # await time_range.set_end(timestamp=end_timestamp)
            await asyncio.gather(
                time_range.set_initial_with_timestamp(timestamp=start_timestamp),
                time_range.set_end(timestamp=end_timestamp),
            )

            start_block = time_range.initial.block
            end_block = time_range.end.block
            start_timestamp = time_range.initial.timestamp
            end_timestamp = time_range.end.timestamp
        else:
            # set timestamp to zero when block is provided
            start_timestamp = 0
            end_timestamp = 0

        # build query
        query = """
        query hypes($block: Int!, $paginate: String!){
            uniswapV3Hypervisors(
                first: 1000
                block: {
                    number: $block
                    }
                orderBy: id
                orderDirection: asc
                where: {
                    id_gt: $paginate
                }
            ){
                grossFeesClaimed0
                grossFeesClaimed1
                grossFeesClaimedUSD
                id
                symbol
                tvl1
                tvl0
                feesReinvested0
                feesReinvested1
                pool{
                    token0{
                        decimals
                        }
                    token1{
                        decimals
                    }
                }
            }
        }
        """

        # retrieve data
        result = {}
        try:
            # paginate results
            initial_hype_status, end_hype_status = await asyncio.gather(
                self.gamma_client.paginate_query(
                    query, "id", {"block": start_block, "paginate": ""}
                ),
                self.gamma_client.paginate_query(
                    query, "id", {"block": end_block, "paginate": ""}
                ),
            )

            initial_hype_status = {hype["id"]: hype for hype in initial_hype_status}

            for end_hype in end_hype_status:
                # if hype id is not present at initial block, set initials to zero
                if end_hype["id"] not in initial_hype_status:
                    initial_hype_status[end_hype["id"]] = {
                        "grossFeesClaimed0": 0,
                        "grossFeesClaimed1": 0,
                        "grossFeesClaimedUSD": 0,
                    }

                # define decimals
                token0_conversion = 10 ** end_hype["pool"]["token0"]["decimals"]
                token1_conversion = 10 ** end_hype["pool"]["token1"]["decimals"]

                result[end_hype["id"]] = {
                    "symbol": end_hype["symbol"],
                    "id": end_hype["id"],
                    "initialBlock": start_block,
                    "initialTimestamp": start_timestamp,
                    "endBlock": end_block,
                    "endTimestamp": end_timestamp,
                    "initialGrossFeesClaimed0": float(
                        initial_hype_status[end_hype["id"]]["grossFeesClaimed0"]
                    )
                    / token0_conversion,
                    "initialGrossFeesClaimed1": float(
                        initial_hype_status[end_hype["id"]]["grossFeesClaimed1"]
                    )
                    / token1_conversion,
                    "initialGrossFeesClaimedUsd": float(
                        initial_hype_status[end_hype["id"]]["grossFeesClaimedUSD"]
                    ),
                    "endGrossFeesClaimed0": float(end_hype["grossFeesClaimed0"])
                    / token0_conversion,
                    "endGrossFeesClaimed1": float(end_hype["grossFeesClaimed1"])
                    / token1_conversion,
                    "endGrossFeesClaimedUsd": float(end_hype["grossFeesClaimedUSD"]),
                    "periodGrossFeesClaimed0": (
                        float(end_hype["grossFeesClaimed0"])
                        - float(
                            initial_hype_status[end_hype["id"]]["grossFeesClaimed0"]
                        )
                    )
                    / token0_conversion,
                    "periodGrossFeesClaimed1": (
                        float(end_hype["grossFeesClaimed1"])
                        - float(
                            initial_hype_status[end_hype["id"]]["grossFeesClaimed1"]
                        )
                    )
                    / token1_conversion,
                    "periodGrossFeesClaimedUsd": float(end_hype["grossFeesClaimedUSD"])
                    - float(initial_hype_status[end_hype["id"]]["grossFeesClaimedUSD"]),
                }

        except Exception as e:
            # check if block is available at subgraph

            if "error" in initial_hype_status:
                raise ValueError(f"{initial_hype_status['error']}")
            if "error" in end_hype_status:
                raise ValueError(f"{end_hype_status['error']}")

            logger.exception(
                f" Unable to retrieve collected fees data for hypervisors from subgraph: {e}"
            )

        # empty result
        return result
