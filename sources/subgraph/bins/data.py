from gql.dsl import DSLQuery
from httpx import HTTPStatusError

from sources.subgraph.bins import LlamaClient
from sources.subgraph.bins.constants import DAY_SECONDS
from sources.subgraph.bins.enums import Chain
from sources.subgraph.bins.hype_fees.schema import Time
from sources.subgraph.bins.subgraphs import SubgraphClient
from sources.subgraph.bins.utils import estimate_block_from_timestamp_diff


class BlockRange:
    """Manage time ranges"""

    def __init__(
        self,
        chain: Chain,
        subgraph_client: SubgraphClient | None = None,
    ) -> None:
        self.chain = chain
        self.initial: Time | None = None
        self.end: Time | None = None
        if subgraph_client:
            self._subgraph_client = subgraph_client
        self._llama_client = LlamaClient(chain)

    async def set_end(self, timestamp: int | None = None) -> None:
        """Set end time and block"""
        if timestamp:
            self.end = await self._get_time_from_timestamp(timestamp)
        else:
            self.end = await self._query_current_time()

    async def set_initial_with_timestamp(self, timestamp: int) -> None:
        """Set initial timestamp and block by providing the timestamp"""
        self.initial = await self._get_time_from_timestamp(timestamp)

    async def set_initial_with_days_ago(self, days_ago: int) -> None:
        """Set initial timestamp and block using days before current time"""
        timestamp_start = self.end.timestamp - (days_ago * DAY_SECONDS)
        try:
            response = await self._llama_client.block_from_timestamp(
                timestamp_start, True
            )
            self.initial = Time(
                block=response["height"], timestamp=response["timestamp"]
            )
        except HTTPStatusError:
            # Estimate start time if not found
            self.initial = Time(
                block=estimate_block_from_timestamp_diff(
                    self.chain,
                    self.end.block,
                    self.end.timestamp,
                    timestamp_start,
                ),
                timestamp=timestamp_start,
            )

    async def _get_time_from_timestamp(self, timestamp: int) -> Time:
        response = await self._llama_client.block_from_timestamp(timestamp, True)
        return Time(block=response["height"], timestamp=response["timestamp"])

    async def _query_current_time(self) -> Time:
        query = DSLQuery(
            self._subgraph_client.data_schema.Query._meta.select(
                self._subgraph_client.meta_fields_fragment()
            )
        )

        response = await self._subgraph_client.execute(query)

        return Time(
            block=response["_meta"]["block"]["number"],
            timestamp=response["_meta"]["block"]["timestamp"],
        )
