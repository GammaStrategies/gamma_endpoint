import logging

import httpx

from sources.common.general.enums import Chain


logger = logging.getLogger(__name__)
async_client = httpx.AsyncClient(
    transport=httpx.AsyncHTTPTransport(
        retries=3,
    ),
    timeout=180,
)


class ramses_api_helper:
    def __init__(self, chain: Chain):
        self.url = self.set_url(chain)
        self._data = None
        self._prices = None

    def set_url(self, chain: Chain) -> str:
        if chain == Chain.ARBITRUM:
            return "https://api-v2-production-a6e6.up.railway.app/mixed-pairs"
        elif chain == Chain.AVALANCHE:
            return "https://pharaoh-api-production.up.railway.app/mixed-pairs"
        elif chain == Chain.MANTLE:
            return "https://cleopatra-api-production.up.railway.app/mixed-pairs"
        else:
            raise ValueError(f"Chain {chain} not supported")

    async def data(self) -> dict:
        if self._data is None:
            tmp_data = await self._get_data()
            self._data = {x["id"].lower(): x for x in tmp_data["pairs"]}
            self._prices = {x["id"].lower(): x for x in tmp_data["tokens"]}
        return self._data

    async def _get_data(self) -> dict:
        response = await async_client.get(self.url)
        response.raise_for_status()
        return response.json()

    async def get_pool_raw_data(self, pool: str) -> dict:
        """Get pool data

        Args:
            pool (str): pool address

        Returns:
                dict: {
                    "averageUsdInRange": 1,
                    "feeApr": 0,
                    "feeDistributor": {
                        "id": "0x5de109a5e2c6fb915f2494b6a056735ee0b15df5",
                        "rewardTokens": [
                            "0x00e1724885473b63bce08a9f0a52f35b0979e35a",
                            "0xa334884bf6b0a066d553d19e507315e839409e62"
                        ]
                    },
                    "feeTier": "10000",
                    "feesPerDay": 0,
                    "feesUSD": 0,
                    "gauge": {
                        "id": "0xf80b56ede950b146456a87257b015a5652d8e488",
                        "isAlive": false,
                        "periodFinish": {},
                        "rewardRate": {},
                        "rewardTokens": [],
                        "xRamRatio": "0"
                    },
                    "id": "0x01ae5c30c29f8954259afa84cb9e9c2ef7971128",
                    "inRangeEmissionApr": 0,
                    "inRangeFeeApr": 0,
                    "initialFee": 10000,
                    "liquidity": 0,
                    "lpApr": 0.0,
                    "oneTickEmissionApr": 0.0,
                    "oneTickFeeApr": 0.0,
                    "pctActiveTvl": 0,
                    "price": 0.04162555278720825,
                    "projectedFees": {
                        "days": 0,
                        "tokens": {
                            "0x00e1724885473b63bce08a9f0a52f35b0979e35a": 0,
                            "0xa334884bf6b0a066d553d19e507315e839409e62": 0
                        }
                    },
                    "recommendedRangesNew": [
                        {
                            "feeApr": 0.0,
                            "inRangeLiquidityUSD": 0.0,
                            "lpApr": 0.0,
                            "name": "Passive",
                            "oneTickTvl": 0.0,
                            "rangeDelta": 2500.0,
                            "rewardApr": 0,
                            "unit": "pct",
                            "value": 50
                        },
                        {
                            "feeApr": 0.0,
                            "inRangeLiquidityUSD": 0.0,
                            "lpApr": 0.0,
                            "name": "Wide",
                            "oneTickTvl": 0.0,
                            "rangeDelta": 750.0,
                            "rewardApr": 0,
                            "unit": "pct",
                            "value": 15
                        },
                        {
                            "feeApr": 0.0,
                            "inRangeLiquidityUSD": 0.0,
                            "lpApr": 0.0,
                            "name": "Narrow",
                            "oneTickTvl": 0.0,
                            "rangeDelta": 500.0,
                            "rewardApr": 0,
                            "unit": "pct",
                            "value": 10
                        },
                        {
                            "default": true,
                            "feeApr": 0.0,
                            "inRangeLiquidityUSD": 0.0,
                            "lpApr": 0.0,
                            "name": "Aggressive",
                            "oneTickTvl": 0.0,
                            "rangeDelta": 250.0,
                            "rewardApr": 0,
                            "unit": "pct",
                            "value": 5
                        }
                    ],
                    "reserve0": 1.0,
                    "reserve1": 1.0,
                    "sqrtPrice": "16164400070433013742070385133",
                    "symbol": "CL-OATH-ERN-1.0%",
                    "tick": "-31792",
                    "tickSpacing": "200",
                    "token0": "0x00e1724885473b63bce08a9f0a52f35b0979e35a",
                    "token1": "0xa334884bf6b0a066d553d19e507315e839409e62",
                    "totalSupply": 0,
                    "totalValueLockedToken0": "0.000000000000000001",
                    "totalValueLockedToken1": "0.000000000000000001",
                    "totalValueLockedUSD": "0.000000000000000001036550214065854999402702474552292",
                    "totalVeShareByPeriod": 0,
                    "tvl": 8.717350688106525e-19,
                    "type": "UNKNOWN",
                    "voteApr": 0,
                    "voteBribes": {
                        "0x00e1724885473b63bce08a9f0a52f35b0979e35a": 0,
                        "0xa334884bf6b0a066d553d19e507315e839409e62": 0
                    },
                    "voteFeeApr": 0
                },
        """
        result = await self.data()
        return result[pool.lower()]

    async def get_price_raw_data(self, token: str) -> dict:
        if self._prices is None:
            await self.data()
        return self._prices[token]

    async def get_pool_apr(self, pool: str, token: str) -> dict:

        result = {}
        tmp_data = await self.get_pool_raw_data(pool)
        if not token in tmp_data["gauge"]["rewardRate"]:
            raise ValueError(f"Token {token} not found in pool {pool}")

        # get price
        tmp_price = await self.get_price_raw_data(token)

        rewards_per_second = (
            tmp_data["gauge"]["rewardRate"][token] / 10 ** tmp_price["decimals"]
        )
        usd_per_day = rewards_per_second * 60 * 60 * 24 * tmp_price["price"]

        # build result
        return {
            "rewardToken": token,
            "rewardRate": tmp_data["gauge"]["rewardRate"][token],
            "price": tmp_price["price"],
            "rewardToken_symbol": tmp_price["symbol"],
            "rewardToken_name": tmp_price["name"],
            "rewardToken_decimals": tmp_price["decimals"],
            "rewardsPerSecond": rewards_per_second,
            "usdPerSecond": rewards_per_second * tmp_price["price"],
            "totalValueLockedUSD": tmp_data["totalValueLockedUSD"],
            "apr": (usd_per_day * 365) / float(tmp_data["totalValueLockedUSD"]),
        }

    async def get_apr(self, pool: str):

        dta = await self.get_pool_raw_data(pool=pool)

        result = {}
        for rewardToken in dta["gauge"]["rewardTokens"]:
            # get price
            tmp_price = await self.get_price_raw_data(rewardToken)

            rewards_per_second = (
                dta["gauge"]["rewardRate"][rewardToken] / 10 ** tmp_price["decimals"]
            )
            usd_per_day = rewards_per_second * 60 * 60 * 24 * tmp_price["price"]

            # build result
            result[rewardToken] = {
                "rewardToken": rewardToken,
                "rewardRate": dta["gauge"]["rewardRate"][rewardToken],
                "price": tmp_price["price"],
                "rewardToken_symbol": tmp_price["symbol"],
                "rewardToken_name": tmp_price["name"],
                "rewardToken_decimals": tmp_price["decimals"],
                "rewardsPerSecond": rewards_per_second,
                "usdPerSecond": rewards_per_second * tmp_price["price"],
                "totalValueLockedUSD": dta["totalValueLockedUSD"],
                "apr": (usd_per_day * 365) / float(dta["totalValueLockedUSD"]),
            }

            # logging.getLogger(__name__).info(
            #     f" {rewardToken} {tmp_price['symbol']} -> reward/day:{rewards_per_second*60*60*24:,.2f} usd/day:${usd_per_day:,.2f} apr: {result[rewardToken]['apr']:,.2%}"
            # )

        return result
