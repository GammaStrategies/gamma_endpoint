from datetime import datetime, timedelta
import logging

from sources.subgraph.bins import GammaClient, UniswapV3Client
from sources.subgraph.bins.config import EXCLUDED_HYPERVISORS
from sources.subgraph.bins.constants import DAYS_IN_PERIOD
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.hype_fees.fees_yield import fee_returns_all
from sources.subgraph.bins.utils import filter_address_by_chain, timestamp_to_date

from sources.subgraph.bins.data import BlockRange


logger = logging.getLogger(__name__)


class HypervisorData:
    def __init__(self, protocol: Protocol, chain: Chain = Chain.MAINNET):
        self.protocol = protocol
        self.chain = chain
        self.gamma_client = GammaClient(protocol, chain)
        self.uniswap_client = UniswapV3Client(protocol, chain)
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
        pool_addresses = [hypervisor["pool"]["id"] for hypervisor in basics]

        query_pool = """
        query slot0($pools: [String!]!){
            pools(
                where: {
                    id_in: $pools
                }
            ) {
                id
                sqrtPrice
                tick
                observationIndex
                feesUSD
                totalValueLockedUSD
            }
        }
        """
        variables = {"pools": pool_addresses}
        pools_response = await self.uniswap_client.query(query_pool, variables)
        pools_data = pools_response["data"]["pools"]
        pools = {pool.pop("id"): pool for pool in pools_data}

        self.basics_data = basics
        self.pools_data = pools

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
            await time_range.set_initial_with_timestamp(timestamp=start_timestamp)
            await time_range.set_end(timestamp=end_timestamp)
            start_block = time_range.initial.block
            end_block = time_range.end.block
            start_timestamp = time_range.initial.timestamp
            end_timestamp = time_range.end.timestamp
        else:
            # set timestamp to zero when block is provided
            start_timestamp = 0
            end_timestamp = 0

        # build queries
        initial_query = """{{
            uniswapV3Hypervisors(
                block: {{number: {} }}
            ) {{
                grossFeesClaimed0
                grossFeesClaimed1
                grossFeesClaimedUSD
                id
                symbol
                tvl1
                tvl0
                feesReinvested0
                feesReinvested1
            }}
        }}
        """.format(
            start_block
        )

        end_query = """{{
            uniswapV3Hypervisors(
                block: {{number: {} }}
            ) {{
                grossFeesClaimed0
                grossFeesClaimed1
                grossFeesClaimedUSD
                id
                symbol
                pool {{
                token0 {{
                    decimals
                }}
                token1 {{
                    decimals
                }}
                }}
            }}
        }}
        """.format(
            end_block
        )

        # retrieve data
        result = {}
        try:
            initial_hype_status = {
                hype["id"]: hype
                for hype in (await self.gamma_client.query(initial_query))["data"][
                    "uniswapV3Hypervisors"
                ]
            }

            for end_hype in (await self.gamma_client.query(end_query))["data"][
                "uniswapV3Hypervisors"
            ]:
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
                    "initial_block": start_block,
                    "initial_timestamp": start_timestamp,
                    "end_block": end_block,
                    "end_timestamp": end_timestamp,
                    "initial_grossFeesClaimed0": float(
                        initial_hype_status[end_hype["id"]]["grossFeesClaimed0"]
                    )
                    / token0_conversion,
                    "initial_grossFeesClaimed1": float(
                        initial_hype_status[end_hype["id"]]["grossFeesClaimed1"]
                    )
                    / token1_conversion,
                    "initial_grossFeesClaimedUSD": float(
                        initial_hype_status[end_hype["id"]]["grossFeesClaimedUSD"]
                    ),
                    "end_grossFeesClaimed0": float(end_hype["grossFeesClaimed0"])
                    / token0_conversion,
                    "end_grossFeesClaimed1": float(end_hype["grossFeesClaimed1"])
                    / token1_conversion,
                    "end_grossFeesClaimedUSD": float(end_hype["grossFeesClaimedUSD"]),
                    "period_grossFeesClaimed0": (
                        float(end_hype["grossFeesClaimed0"])
                        - float(
                            initial_hype_status[end_hype["id"]]["grossFeesClaimed0"]
                        )
                    )
                    / token0_conversion,
                    "period_grossFeesClaimed1": (
                        float(end_hype["grossFeesClaimed1"])
                        - float(
                            initial_hype_status[end_hype["id"]]["grossFeesClaimed1"]
                        )
                    )
                    / token1_conversion,
                    "period_grossFeesClaimedUSD": float(end_hype["grossFeesClaimedUSD"])
                    - float(initial_hype_status[end_hype["id"]]["grossFeesClaimedUSD"]),
                }

        except Exception as e:
            logger.debug(
                f" Unable to retrieve collected fees data for hypervisors from subgraph: {e}"
            )

        # empty result
        return result


class HypervisorInfo(HypervisorData):
    def empty_returns(self):
        return {
            period: {
                "feeApr": 0,
                "feeApy": 0,
            }
            for period in DAYS_IN_PERIOD
        }

    async def basic_stats(self, hypervisor_address):
        data = await self._get_hypervisor_data(hypervisor_address)
        return data

    async def all_data(self, get_data=True):
        if get_data:
            await self._get_all_data()

        basics = self.basics_data
        pools = self.pools_data

        fee_yield_output = await fee_returns_all(
            protocol=self.protocol,
            chain=self.chain,
            days=1,
            hypervisors=None,
            current_timestamp=None,
        )

        returns = {
            hypervisor: {
                "daily": {
                    "feeApr": hypervisor_returns["feeApr"],
                    "feeApy": hypervisor_returns["feeApy"],
                },
                "weekly": {
                    "feeApr": hypervisor_returns["feeApr"],
                    "feeApy": hypervisor_returns["feeApy"],
                },
                "monthly": {
                    "feeApr": hypervisor_returns["feeApr"],
                    "feeApy": hypervisor_returns["feeApy"],
                },
                "allTime": {
                    "feeApr": hypervisor_returns["feeApr"],
                    "feeApy": hypervisor_returns["feeApy"],
                },
                "status": hypervisor_returns.get("hasOutlier")
                if hypervisor_returns.get("hasOutlier")
                else hypervisor_returns["status"],
            }
            for hypervisor, hypervisor_returns in fee_yield_output.items()
        }

        results = {}
        for hypervisor in basics:
            try:
                hypervisor_id = hypervisor["id"]
                symbol0 = hypervisor["pool"]["token0"]["symbol"]
                symbol1 = hypervisor["pool"]["token1"]["symbol"]
                hypervisor_name = f'{symbol0}-{symbol1}-{hypervisor["pool"]["fee"]}'
                pool_id = hypervisor["pool"]["id"]
                decimals0 = hypervisor["pool"]["token0"]["decimals"]
                decimals1 = hypervisor["pool"]["token1"]["decimals"]
                tick = int(pools[pool_id]["tick"]) if pools[pool_id]["tick"] else 0
                baseLower = int(hypervisor["baseLower"])
                baseUpper = int(hypervisor["baseUpper"])
                totalSupply = int(hypervisor["totalSupply"])
                maxTotalSupply = int(hypervisor["maxTotalSupply"])
                capacityUsed = (
                    totalSupply / maxTotalSupply if maxTotalSupply > 0 else "No cap"
                )

                results[hypervisor_id] = {
                    "createDate": timestamp_to_date(
                        int(hypervisor["created"]), "%d %b, %Y"
                    ),
                    "poolAddress": pool_id,
                    "name": hypervisor_name,
                    "token0": hypervisor["pool"]["token0"]["id"],
                    "token1": hypervisor["pool"]["token1"]["id"],
                    "decimals0": decimals0,
                    "decimals1": decimals1,
                    "depositCap0": int(hypervisor["deposit0Max"]) / 10**decimals0,
                    "depositCap1": int(hypervisor["deposit1Max"]) / 10**decimals1,
                    "grossFeesClaimed0": int(hypervisor["grossFeesClaimed0"])
                    / 10**decimals0,
                    "grossFeesClaimed1": int(hypervisor["grossFeesClaimed1"])
                    / 10**decimals1,
                    "grossFeesClaimedUSD": hypervisor["grossFeesClaimedUSD"],
                    "feesReinvested0": int(hypervisor["feesReinvested0"])
                    / 10**decimals0,
                    "feesReinvested1": int(hypervisor["feesReinvested1"])
                    / 10**decimals1,
                    "feesReinvestedUSD": hypervisor["feesReinvestedUSD"],
                    "tvl0": int(hypervisor["tvl0"]) / 10**decimals0,
                    "tvl1": int(hypervisor["tvl1"]) / 10**decimals1,
                    "tvlUSD": hypervisor["tvlUSD"],
                    "totalSupply": totalSupply,
                    "maxTotalSupply": maxTotalSupply,
                    "capacityUsed": capacityUsed,
                    "sqrtPrice": pools[pool_id]["sqrtPrice"],
                    "tick": tick,
                    "baseLower": baseLower,
                    "baseUpper": baseUpper,
                    "inRange": bool(baseLower <= tick <= baseUpper),
                    "observationIndex": pools[pool_id]["observationIndex"],
                    "poolTvlUSD": pools[pool_id]["totalValueLockedUSD"],
                    "poolFeesUSD": pools[pool_id]["feesUSD"],
                    "returns": returns.get(hypervisor_id, self.empty_returns()),
                }
            except Exception as e:
                logger.warning(f"Failed on hypervisor {hypervisor['id']}")
                logger.exception(e)
                pass

        return results

    # TODO: specify chain on hardcoded overrides
    def apply_returns_overrides(self, hypervisor_address, returns):
        if hypervisor_address == "0x717a3276bd6f9e2f0ae447e0ffb45d0fa1c2dc57":
            returns["daily"] = {
                "totalPeriodSeconds": 629817,
                "cumFeeReturn": 6.317775,
                "feeApr": 0.0168880097306676,
                "feeApy": 0.01703102099,
            }
        if hypervisor_address in [
            "0x3cca05926af387f1ab4cd45ce8975d31f0469927",
            "0x717a3276bd6f9e2f0ae447e0ffb45d0fa1c2dc57",
        ]:
            print("override")
            returns["weekly"] = returns["daily"]

        return returns
