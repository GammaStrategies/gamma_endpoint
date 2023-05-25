import datetime

from sources.subgraph.bins import UniswapV3Client
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.utils import sqrtPriceX96_to_priceDecimal


class Pool:
    def __init__(self, protocol: Protocol, chain: Chain = Chain.ETHEREUM):
        self.client = UniswapV3Client(protocol, chain)

    async def swap_prices(self, pool_address, time_delta=None):
        query = """
        query poolPrices($pool: String!, $timestampStart: Int!, $paginate: String!){
            swaps(
                first: 1000
                pool: $pool
                orderBy: id
                orderDirection: asc
                where: {
                    timestamp_gte: $timestampStart
                    id_gt: $paginate
                }
            ){
                id
                timestamp
                sqrtPriceX96
            }
        }
        """
        if time_delta:
            timestamp_start = int(
                (datetime.datetime.now(datetime.timezone.utc) - time_delta)
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
            )

        else:
            timestamp_start = 0

        variables = {
            "pool": pool_address,
            "timestampStart": timestamp_start,
            "paginate": "",
        }
        data = await self.client.paginate_query(query, "id", variables)
        return data

    async def hourly_prices(self, pools, hours):
        query = """
        query poolPrices($pools: [String!]!, $hours: Int!){
            pools(
                where: {
                    id_in: $pools
                    }
                ){
                    id
                    token0 {
                        decimals
                    }
                    token1 {
                        decimals
                    }
                    poolHourData(
                        first: $hours
                        orderBy: id
                        orderDirection: desc
                        where: {
                            sqrtPrice_gt: 0
                        }
                    ){
                        periodStartUnix
                        sqrtPrice
                    }
                }
            }
        """
        variables = {"pools": [pool.lower() for pool in pools], "hours": hours}
        response = await self.client.query(query, variables)
        data = response["data"]["pools"]

        return {
            pool["id"]: [
                {
                    "timestamp": hour_data["periodStartUnix"],
                    "price": sqrtPriceX96_to_priceDecimal(
                        float(hour_data["sqrtPrice"]),
                        int(pool["token0"]["decimals"]),
                        int(pool["token1"]["decimals"]),
                    ),
                }
                for hour_data in pool["poolHourData"]
            ]
            for pool in data
        }
