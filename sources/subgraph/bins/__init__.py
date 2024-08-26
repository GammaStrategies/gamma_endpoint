import logging

import httpx

from sources.subgraph.bins.config import gamma_subgraph_ids
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.subgraphs import (
    Service,
    StudioService,
    GoldskyService,
    SentioService,
    UrlService,
)


logger = logging.getLogger(__name__)
async_client = httpx.AsyncClient(
    transport=httpx.AsyncHTTPTransport(
        retries=3,
    ),
    timeout=180,
)


class SubgraphClient:
    def __init__(self, subgraph_id: str, chain: Chain = Chain.ETHEREUM):
        self.chain = chain
        self.parse_subgraph_id(subgraph_id)
        self._url = self.service.url()

    def parse_subgraph_id(self, subgraph_id: str) -> None:
        """Parse out service and subgraph ID"""
        service, parsed_id = subgraph_id.split("::")

        self.subgraph_id = parsed_id

        if service == Service.STUDIO:
            self.service = StudioService(self.subgraph_id)
        elif service == Service.GOLDSKY:
            self.service = GoldskyService(self.subgraph_id)
        elif service == Service.SENTIO:
            self.service = SentioService(self.subgraph_id)
        else:
            self.service = UrlService(self.subgraph_id)

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
        response = await async_client.post(
            self._url, json=params, headers=self.service.headers()
        )

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
    def __init__(self, protocol: Protocol, chain: Chain):
        super().__init__(gamma_subgraph_ids[protocol][chain], chain)


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
