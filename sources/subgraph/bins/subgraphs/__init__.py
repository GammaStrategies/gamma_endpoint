import logging
from functools import wraps

from gql import Client as GqlClient
from gql.dsl import DSLFragment, DSLQuery, DSLSchema, dsl_gql
from gql.transport.aiohttp import AIOHTTPTransport, log as requests_logger

from sources.subgraph.bins.config import GQL_CLIENT_TIMEOUT

requests_logger.setLevel(logging.WARNING)


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

    def __init__(self, url: str, schema, execute_timeout: int) -> None:
        self.url = url
        super().__init__(
            schema=schema,
            transport=AIOHTTPTransport(url=url),
            execute_timeout=execute_timeout,
        )


class SubgraphClient:
    """Subgraph base client to manage query execution and shared fragments"""

    def __init__(self, url: str, schema_path: str) -> None:
        with open(schema_path, encoding="utf-8") as schema_file:
            schema = schema_file.read()
        self.client = AsyncGqlClient(
            url=url, schema=schema, execute_timeout=GQL_CLIENT_TIMEOUT
        )
        self.data_schema = DSLSchema(self.client.schema)
        self._fragment_dependencies: list[DSLFragment] = []
        self._fragments_used: list[str] = []

    async def execute(self, query: DSLQuery) -> dict:
        """Executes query and returns result"""
        gql = dsl_gql(*self._fragment_dependencies, query)
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
