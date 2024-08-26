import logging
from abc import ABC, abstractmethod
from enum import Enum
from functools import wraps
from typing import Any

from gql import Client as GqlClient
from gql.dsl import DSLFragment, DSLQuery, DSLSchema, dsl_gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.aiohttp import log as requests_logger

from sources.subgraph.bins.config import (
    GQL_CLIENT_TIMEOUT,
    SUBGRAPH_STUDIO_KEY,
    SUBGRAPH_STUDIO_USER_KEY,
    GOLDSKY_PROJECT_NAME,
    SENTIO_ACCOUNT,
    SENTIO_KEY,
)

requests_logger.setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class Service(str, Enum):
    STUDIO = "studio"
    GOLDSKY = "goldsky"
    SENTIO = "sentio"
    URL = "url"


class SubgraphService(ABC):
    def __init__(self, service: Service, subgraph_id: str, api_key: str | None = None):
        self.service = service
        self.subgraph_id = subgraph_id
        self.api_key = api_key

    @abstractmethod
    def url(self) -> str:
        """Returns query endpoint url"""

    def headers(self) -> str | None:
        """Returns headers"""
        return None


class StudioService(SubgraphService):
    def __init__(self, subgraph_id: str):
        super().__init__(Service.STUDIO, subgraph_id, SUBGRAPH_STUDIO_KEY)

    def url(self) -> str:
        base_url = "https://gateway-arbitrum.network.thegraph.com/api"

        return f"{base_url}/{self.api_key}/deployments/id/{self.subgraph_id}"


class GoldskyService(SubgraphService):
    def __init__(self, subgraph_id: str):
        super().__init__(Service.STUDIO, subgraph_id)

    def url(self) -> str:
        base_url = "https://api.goldsky.com/api/public"

        return f"{base_url}/{GOLDSKY_PROJECT_NAME}/subgraphs/{self.subgraph_id}/latest/gn"


class SentioService(SubgraphService):
    def __init__(self, subgraph_id: str):
        super().__init__(Service.STUDIO, subgraph_id, SENTIO_KEY)

    def url(self) -> str:
        base_url = "https://app.sentio.xyz/api/v1/graphql"

        return f"{base_url}/{SENTIO_ACCOUNT}/{self.subgraph_id}"

    def headers(self) -> dict:
        return {"api-key": self.api_key}


class UrlService(SubgraphService):
    def __init__(self, subgraph_id: str):
        super().__init__(Service.URL, subgraph_id)

    def url(self) -> str:
        return self.subgraph_id


def fragment(fragment_function):
    """
    Decorator for use with SubgraphClient methods keep track of fragment usage
    All fragment methods should be decorated with this
    """

    @wraps(fragment_function)
    def wrapper(*args):
        frag = fragment_function(*args)
        instance = args[0]  # self
        if frag.name not in instance._fragments_used:
            instance._fragments_used.append(frag.name)
            instance._fragment_dependencies.append(frag)
        return frag

    return wrapper


class AsyncGqlClient(GqlClient):
    """Subclass of gql Client that defaults to AIOHTTPTransport"""

    def __init__(
        self, url: str, schema, execute_timeout: int, headers: dict | None = None
    ) -> None:
        self.url = url
        super().__init__(
            schema=schema,
            transport=AIOHTTPTransport(url=url, headers=headers),
            execute_timeout=execute_timeout,
        )


class SubgraphClient:
    """Subgraph base client to manage query execution and shared fragments"""

    def __init__(self, schema_path: str, subgraph_id: str) -> None:
        with open(schema_path, encoding="utf-8") as schema_file:
            schema = schema_file.read()

        self.parse_subgraph_id(subgraph_id)

        self.client = AsyncGqlClient(
            url=self.service.url(),
            schema=schema,
            execute_timeout=GQL_CLIENT_TIMEOUT,
            headers=self.service.headers(),
        )
        self.data_schema = DSLSchema(self.client.schema)
        self._fragment_dependencies: list[DSLFragment] = []
        self._fragments_used: list[str] = []

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


    async def execute(self, query: DSLQuery) -> dict:
        """Executes query and returns result"""
        gql = dsl_gql(*self._fragment_dependencies, query)

        logger.debug("Subgraph call to %s", self.client.url)
        async with self.client as session:
            result = await session.execute(gql)
            return result

    @fragment
    def meta_fields_fragment(self) -> DSLFragment:
        """Meta fragment is common across all subgraphs"""
        ds = self.data_schema
        frag = DSLFragment("MetaFields")
        frag.on(ds._Meta_)
        frag.select(ds._Meta_.block.select(ds._Block_.number, ds._Block_.timestamp))
        return frag


class SubgraphData(ABC):
    """Abstract base class for subgraph data."""

    def __init__(self):
        self.data: Any
        self.query_response: dict

    def load_query_response(self, query_response: dict) -> None:
        """Load data from external source to skip querying.

        Args:
            query_response: dict with data from subgraph query
        """
        self.query_response = query_response

    async def get_data(self, run_query: bool = True, **kwargs) -> None:
        """Get data, transforms it and stores it in self.data.

        Args:
            run_query: Defaults to True, set to False if data is already loaded
        """
        if run_query:
            await self._query_data(**kwargs)

        self.data = self._transform_data()

    @abstractmethod
    async def _query_data(self) -> dict:
        """Query subgraph and sets self.query_response."""
        # query = ""
        # response = await self.client.execute(query)
        # self.query_response = response

    @abstractmethod
    def _transform_data(self) -> Any:
        """Transformations for self.query_response into self.data"""
        self.data = self.query_response
