import logging

import httpx

from sources.subgraph.bins.config import (
    ETH_BLOCKS_SUBGRAPH_URL,
    THEGRAPH_INDEX_NODE_URL,
    UNI_V2_SUBGRAPH_URL,
    XGAMMA_SUBGRAPH_URL,
    dex_hypepool_subgraph_urls,
    gamma_subgraph_urls,
)
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.subgraphs.gamma import get_subgraph_studio_key

logger = logging.getLogger(__name__)
async_client = httpx.AsyncClient(
    transport=httpx.AsyncHTTPTransport(
        retries=3,
    ),
    timeout=180,
)


class SubgraphClient:
    def __init__(self, url: str, chain: Chain = Chain.ETHEREUM):
        self._url = url
        self.chain = chain

    def studio_url(self, subgraph_id: str, api_key):
        if subgraph_id.startswith("http"):
            return subgraph_id

        base_url = "https://gateway-arbitrum.network.thegraph.com/api/"

        return f"{base_url}{api_key}/deployments/id/{subgraph_id}"

    async def query(self, query: str, variables=None) -> dict:
        """Make graphql query to subgraph"""
        if variables:
            params = {"query": query, "variables": variables}
        else:
            params = {"query": query}
        # TODO: error handling -> connection, result and others
        #       httpcore.RemoteProtocolError
        # ssl.SSLError:
        # [SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure
        #
        response = await async_client.post(self._url, json=params)

        logger.debug("Subgraph call to %s", self._url)

        if response.status_code == 200:
            try:
                response_json = response.json()

                if error_messages := response_json.get("errors"):
                    logger.error("Error while querying %s", self._url)
                    for n, msg in enumerate(error_messages):
                        logger.error("Error #%s - %s", n, msg.get("message", ""))
                    raise ValueError
                return response_json
            except Exception:
                logger.error(
                    " Unexpected error while converting response to json. resp.text: %s",
                    response.text,
                )
        else:
            # handle bad status code
            # Can expand this to handle specific codes once we have specific examples
            logger.error(
                "Unexpected response code %s received  resp.text: %s ",
                response.status_code,
                response.text,
            )

        # error return
        return {}

    async def paginate_query(self, query, paginate_variable, variables={}):
        # if not variables:
        #     variables = {}

        if f"{paginate_variable}_gt" not in query:
            raise ValueError("Paginate variable missing in query")

        variables["orderBy"] = paginate_variable
        variables["orderDirection"] = "asc"

        all_data = []
        has_data = True
        params = {"query": query, "variables": variables}
        while has_data:
            response = await async_client.post(self._url, json=params)
            data = next(iter(response.json()["data"].values()))
            has_data = bool(data)
            if has_data:
                all_data += data
                params["variables"]["paginate"] = data[-1][paginate_variable]

        return all_data


class GammaClient(SubgraphClient):
    def __init__(self, protocol: Protocol, chain: Chain, api_key: str = "prod"):
        super().__init__(
            self.studio_url(
                gamma_subgraph_urls[protocol][chain], get_subgraph_studio_key(api_key)
            ),
            chain,
        )


class UniswapV2Client(SubgraphClient):
    def __init__(self):
        super().__init__(UNI_V2_SUBGRAPH_URL)


class HypePoolClient(SubgraphClient):
    def __init__(self, protocol: Protocol, chain: Chain, api_key: str = "prod"):
        super().__init__(
            self.studio_url(
                dex_hypepool_subgraph_urls[protocol][chain],
                get_subgraph_studio_key(api_key),
            ),
            chain,
        )


class EthBlocksClient(SubgraphClient):
    def __init__(self):
        super().__init__(ETH_BLOCKS_SUBGRAPH_URL)

    def block_from_timestamp(self, timestamp):
        """Get closest from timestamp"""
        ten_minutes_in_seconds = 600
        query = """
        query blockQuery($startTime: Int!, $endTime:Int!){
          blocks(first: 1, orderBy: timestamp, orderDirection: asc,
                 where: {timestamp_gt: $startTime, timestamp_lt: $endTime}) {
            id
            number
            timestamp
          }
        }
        """

        variables = {
            "startTime": timestamp,
            "endTime": timestamp + ten_minutes_in_seconds,
        }

        return int(self.query(query, variables)["data"]["blocks"][0]["number"])


class IndexNodeClient(SubgraphClient):
    def __init__(self, protocol: Protocol, chain: Chain):
        super().__init__(THEGRAPH_INDEX_NODE_URL)
        self.url = gamma_subgraph_urls[protocol][chain]
        self.set_subgraph_name()

    def set_subgraph_name(self):
        split_subgraph_url = self.url.split("/")
        if not split_subgraph_url[-1]:
            split_subgraph_url.pop(-1)
        self.subgraph_name = f"{split_subgraph_url[-2]}/{split_subgraph_url[-1]}"

    async def status(self):
        query = f"""
        {{
            indexingStatusForCurrentVersion(
                subgraphName: "{self.subgraph_name}"
            ){{
                chains{{
                    latestBlock {{ hash number }}
                }}
            }}
        }}
        """

        response = await self.query(query)
        latestBlock = int(
            response["data"]["indexingStatusForCurrentVersion"]["chains"][0][
                "latestBlock"
            ]["number"]
        )

        return {"url": self.url, "latestBlock": latestBlock}


class XgammaClient(SubgraphClient):
    def __init__(self):
        super().__init__(XGAMMA_SUBGRAPH_URL)


class CoingeckoClient:
    """Client for interacting with Coingecko API"""

    def __init__(self):
        self.base = "https://api.coingecko.com/api/v3/"

    async def get_price(self, ids, vs_currencies):
        endpoint = f"{self.base}/simple/price"

        params = {"ids": ids, "vs_currencies": vs_currencies}

        response = await async_client.get(endpoint, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            return {"gamma-strategies": {"usd": 0.623285, "eth": 0.00016391}}


class LlamaClient:
    """Client for interacting with DefiLlama API"""

    def __init__(self, chain: Chain):
        self.base = "https://coins.llama.fi"
        self.chain = self._translate_chain_name(chain)

    def _translate_chain_name(self, chain):
        mapping = {Chain.ETHEREUM: "ethereum"}
        return mapping.get(chain, chain)

    async def block_from_timestamp(self, timestamp, return_timestamp=False):
        """Get closest block number given a unix timestamp"""
        endpoint = f"{self.base}/block/{self.chain}/{timestamp}"

        response = await async_client.get(endpoint)

        response.raise_for_status()

        if return_timestamp:
            return response.json()
        return response.json()["height"]

    async def current_token_price_multi(self, token_list: list[str]) -> dict:
        """Requests multiple token current price"""
        if not token_list:
            return {}

        coins = ",".join([f"{self.chain}:{token}" for token in token_list])
        endpoint = f"{self.base}/prices/current/{coins}"

        response = await async_client.get(endpoint)

        response.raise_for_status()

        response_coins = response.json()["coins"]

        prices = {
            coin_identifier.split(":")[1]: data.get("price", 0)
            for coin_identifier, data in response_coins.items()
        }

        return prices
