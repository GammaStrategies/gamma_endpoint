import asyncio
import re
import typing
from fastapi import Query, Response, APIRouter, status
from fastapi.responses import StreamingResponse
from fastapi.routing import APIRoute
from fastapi_cache.decorator import cache
from endpoint.config.cache import DB_CACHE_TIMEOUT, DAILY_CACHE_TIMEOUT

from endpoint.routers.template import (
    router_builder_generalTemplate,
    router_builder_baseTemplate,
)
from sources.common.general.enums import Chain, Protocol, int_to_chain
from sources.common.general.utils import filter_addresses
from sources.internal.bins.user import get_user_addresses, get_user_shares
from sources.mongo.bins.apps import hypervisor
from sources.mongo.bins.apps import user
from sources.mongo.bins.apps import prices
from sources.mongo.bins.apps import rewards


DEPLOYED: list[tuple[Protocol, Chain]] = [
    (Protocol.UNISWAP, Chain.ETHEREUM),
    (Protocol.UNISWAP, Chain.POLYGON),
    (Protocol.UNISWAP, Chain.ARBITRUM),
    (Protocol.UNISWAP, Chain.OPTIMISM),
    (Protocol.UNISWAP, Chain.CELO),
    (Protocol.UNISWAP, Chain.BSC),
    (Protocol.UNISWAP, Chain.MOONBEAM),
    (Protocol.UNISWAP, Chain.AVALANCHE),
    (Protocol.QUICKSWAP, Chain.POLYGON),
    (Protocol.QUICKSWAP, Chain.POLYGON_ZKEVM),
    (Protocol.QUICKSWAP, Chain.ASTAR_ZKEVM),
    (Protocol.QUICKSWAP, Chain.IMMUTABLE_ZKEVM),
    (Protocol.QUICKSWAP_UNISWAP, Chain.POLYGON_ZKEVM),
    (Protocol.ZYBERSWAP, Chain.ARBITRUM),
    (Protocol.THENA, Chain.BSC),
    (Protocol.THENA, Chain.OPBNB),
    (Protocol.CAMELOT, Chain.ARBITRUM),
    (Protocol.GLACIER, Chain.AVALANCHE),
    (Protocol.RETRO, Chain.POLYGON),
    (Protocol.STELLASWAP, Chain.MOONBEAM),
    (Protocol.BEAMSWAP, Chain.MOONBEAM),
    (Protocol.SPIRITSWAP, Chain.FANTOM),
    (Protocol.SUSHI, Chain.POLYGON),
    (Protocol.SUSHI, Chain.ARBITRUM),
    (Protocol.SUSHI, Chain.BASE),
    (Protocol.RAMSES, Chain.ARBITRUM),
    (Protocol.ASCENT, Chain.POLYGON),
    (Protocol.FUSIONX, Chain.MANTLE),
    (Protocol.SYNTHSWAP, Chain.BASE),
    (Protocol.LYNEX, Chain.LINEA),
    (Protocol.PEGASYS, Chain.ROLLUX),
    (Protocol.BASEX, Chain.BASE),
    (Protocol.PANCAKESWAP, Chain.ARBITRUM),
    (Protocol.APERTURE, Chain.MANTA),
    (Protocol.QUICKSWAP, Chain.MANTA),
    (Protocol.HERCULES, Chain.METIS),
    (Protocol.BASESWAP, Chain.BASE),
    (Protocol.SWAPBASED, Chain.BASE),
    (Protocol.PHARAOH, Chain.AVALANCHE),
    (Protocol.SWAPR, Chain.GNOSIS),
    (Protocol.THICK, Chain.BASE),
    (Protocol.CLEOPATRA, Chain.MANTLE),
    (Protocol.BLASTER, Chain.BLAST),
]


def build_routers() -> list:
    routes = []

    # add global route

    # setup protocol + chain endpoints
    for protocol, chain in DEPLOYED:
        routes.append(
            mongo_router_builder(
                protocol=protocol,
                chain=chain,
                tags=[f"{protocol.fantasy_name} - {chain.fantasy_name}"],
                prefix=f"/{protocol.api_url}/{chain.api_url}",
            )
        )

    return routes


class mongo_router_builder_general(router_builder_baseTemplate):
    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        return self._create_routes()

    def _create_routes(self) -> APIRouter:
        """Create routes for the given chain and protocol combination."""

        router = APIRouter()

        # ROOT
        router.add_api_route(
            path=f"{self.prefix}/",
            endpoint=self.root,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    # EXECUTION FUNCTIONS

    @cache(expire=DB_CACHE_TIMEOUT)
    async def total_tvl(
        self,
        response: Response,
        # timestamp: int | None = None,
        # block: int | None = None,
    ):
        pass
        # return await hypervisor.hypervisor_uncollected_fees(
        #     network=self.chain,
        #     hypervisor_address=hypervisor_address,
        #     timestamp=timestamp,
        #     block=block,
        # )


class mongo_router_builder(router_builder_baseTemplate):
    def __init__(
        self,
        protocol: Protocol,
        chain: Chain,
        tags: list | None = None,
        prefix: str = "",
    ):
        super().__init__(tags=tags, prefix=prefix)

        self.protocol = protocol
        self.chain = chain
        # set tags if not supplied
        self.tags = self.tags or [f"{chain.fantasy_name} - {protocol.fantasy_name}"]
        self.name = type(self).__name__

    def generate_unique_id(self, route: "APIRoute") -> str:
        operation_id = f"{self.name}_{self.tags}_{route.name + route.path_format}"
        operation_id = re.sub(r"\W", "_", operation_id)
        assert route.methods
        operation_id = operation_id + "_" + list(route.methods)[0].lower()
        return operation_id

    # ROUTEs BUILD FUNCTIONS
    def router(self) -> APIRouter:
        return self._create_routes()

    def _create_routes(self) -> APIRouter:
        """Create routes for the given chain and protocol combination."""

        router = APIRouter()

        # ROOT
        router.add_api_route(
            path=f"{self.prefix}/",
            endpoint=self.root,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        # create all other routes
        router = self._create_routes_hypervisor(router=router)

        router = self._create_routes_hypervisors(router=router)

        router = self._create_routes_user(router=router)

        router.add_api_route(
            path=f"{self.prefix}{'/prices'}",
            endpoint=self.prices,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    def _create_routes_hypervisor(
        self,
        router: APIRouter,
    ) -> APIRouter:
        """Create /hypervisor routes for the given chain and dex combination."""

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/collectedFees'}",
            endpoint=self.hypervisor_collected_fees,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/uncollectedFees'}",
            endpoint=self.hypervisor_uncollected_fees,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/prices'}",
            endpoint=self.hypervisor_prices,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisor/{hypervisor_address}/rewards'}",
            endpoint=self.hypervisor_rewards,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    def _create_routes_hypervisors(
        self,
        router: APIRouter,
    ) -> APIRouter:
        # add hypervisor list
        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors'}",
            endpoint=self.hypervisors_list,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/lastSnapshot'}",
            endpoint=self.hypervisors_last_snapshot,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/collectedFees'}",
            endpoint=self.hypervisors_collected_fees,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/uncollectedFees'}",
            endpoint=self.hypervisors_uncollected_fees,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/rewards'}",
            endpoint=self.hypervisors_rewards_status,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/hypervisors/rewards/mfd'}",
            endpoint=self.hypervisors_latest_multiFeeDistribution_rewards,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    def _create_routes_user(self, router: APIRouter) -> APIRouter:

        router.add_api_route(
            path=f"{self.prefix}{'/user/list'}",
            endpoint=self.user_list_addresses,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        router.add_api_route(
            path=f"{self.prefix}{'/user/{user_address}'}",
            endpoint=self.user_data,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        # router.add_api_route(
        #     path=f"{self.prefix}{'/user/{user_address}/analytics'}",
        #     endpoint=self.user_analytics,
        #     methods=["GET"],
        #     generate_unique_id_function=self.generate_unique_id,
        # )

        router.add_api_route(
            path=f"{self.prefix}{'/user/{user_address}/shares'}",
            endpoint=self.user_shares,
            methods=["GET"],
            generate_unique_id_function=self.generate_unique_id,
        )

        return router

    # EXECUTION FUNCTIONS

    def root(self) -> str:
        return f"Gamma Strategies on {self.chain.fantasy_name}'s {self.protocol.fantasy_name} "

    # Hypervisor

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisor_uncollected_fees(
        self,
        hypervisor_address: str,
        response: Response,
        timestamp: int | None = None,
        block: int | None = None,
    ):
        return await hypervisor.hypervisor_uncollected_fees(
            network=self.chain,
            hypervisor_address=hypervisor_address,
            timestamp=timestamp,
            block=block,
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisor_collected_fees(
        self,
        hypervisor_address: str,
        response: Response,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ):
        return await hypervisor.hypervisor_collected_fees(
            network=self.chain,
            hypervisor_address=hypervisor_address,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            start_block=start_block,
            end_block=end_block,
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisor_prices(
        self,
        hypervisor_address: str,
        response: Response,
    ):
        """ """
        return await hypervisor.get_hypervisor_prices(
            network=self.chain, hypervisor_address=hypervisor_address
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisor_rewards(
        self,
        hypervisor_address: str,
        response: Response,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ):
        return await hypervisor.get_hypervisor_rewards_status(
            network=self.chain,
            hypervisor_address=hypervisor_address,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            start_block=start_block,
            end_block=end_block,
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def prices(
        self,
        response: Response,
        token_addresses: typing.List[str] = Query(None),
        block: int | None = None,
    ):
        """
        * When **no token addresses nor block** are provided, it returns the current prices for all tokens in the network (current block).

        * When **token addresses** are provided, it returns their prices at the last operation known (last operation block).

        * When **block** is provided, it returns the prices for all tokens in the network at that block.
        """
        if token_addresses is None:
            if block is None:
                return await prices.get_current_prices(
                    network=self.chain, token_addresses=token_addresses
                )
            else:
                return await prices.get_prices(
                    network=self.chain, token_addresses=None, block=block
                )
            #    return "Please provide a list of token addresses or a block number"
        if block is None:
            return await prices.get_current_prices(
                token_addresses=[x.lower().strip() for x in token_addresses],
                network=self.chain,
            )
        else:
            return await prices.get_prices(
                token_addresses=[x.lower().strip() for x in token_addresses],
                network=self.chain,
                block=block,
            )

    # Hypervisors
    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_list(self, response: Response):
        """Returns the hypervisor found in the database"""
        return await hypervisor.hypervisors_list(
            network=self.chain, protocol=self.protocol
        )

    @cache(expire=DAILY_CACHE_TIMEOUT)
    async def hypervisors_last_snapshot(
        self,
        response: Response,
        address: str | None = Query(None, description="hypervisor address"),
    ):
        """Returns the hypervisor found in the database"""

        if address:
            address = filter_addresses(address)

        return await hypervisor.hypervisors_last_snapshot(
            network=self.chain, protocol=self.protocol, hypervisor_address=address
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_uncollected_fees(
        self,
        response: Response,
    ):
        return await hypervisor.hypervisors_uncollected_fees(network=self.chain)

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_collected_fees(
        self,
        response: Response,
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        start_block: int | None = None,
        end_block: int | None = None,
    ):
        return await hypervisor.hypervisors_collected_fees(
            network=self.chain,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            start_block=start_block,
            end_block=end_block,
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_rewards_status(
        self,
        response: Response,
    ):
        """
        Returns the latest known by database rewards for all hypervisors in the network:
        ***(Be aware that rewards are calculated on every deposit, withraw, rebalance, zeroFee hypervisor operation)***
        """
        return await hypervisor.hypervisors_rewards_status(
            network=self.chain, protocol=self.protocol
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def hypervisors_latest_multiFeeDistribution_rewards(self, response: Response):
        """
        Returns the latest known by database multiFee distribution contract rewards
        * 'stakedAmount' is the total amount of LP tokens staked in the multiFeeDistribution contract
        * 'baseRewardsSinceLastUpdateTime' rewards uncollected since last update
        * 'boostRewardsSinceLastUpdateTime' rewards uncollected since last update
        * 'seconds_sinceLastUpdateTime' seconds passed since last update ( to timestamp field)
        """
        return await rewards.latest_multifeeDistributor(
            network=self.chain, protocol=self.protocol
        )

    # User
    @cache(expire=DB_CACHE_TIMEOUT)
    async def user_data(self, user_address: str, response: Response):
        return await user.get_user_historic_info(
            chain=self.chain, address=user_address.lower()
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def user_analytics(self, user_address: str, response: Response):
        return await user.get_user_analytic_data(
            chain=self.chain, address=user_address.lower()
        )

        # USER MERKL REWARD RELATED #################################################################

    @cache(expire=DB_CACHE_TIMEOUT)
    async def user_list_addresses(
        self,
        response: Response,
        hypervisor_address: str | None = Query(None, description="hypervisor address"),
    ):
        """Returns known user addresses"""

        return await get_user_addresses(
            chain=self.chain,
            hypervisor_address=hypervisor_address
            or [
                x["address"]
                for x in await hypervisor.hypervisors_list(
                    network=self.chain, protocol=self.protocol
                )
            ],
        )

    @cache(expire=DB_CACHE_TIMEOUT)
    async def user_shares(
        self,
        response: Response,
        user_address: str,
        hypervisor_address: str | None = Query(None, description="hypervisor address"),
        timestamp_ini: int | None = Query(
            None, description="will limit the data returned from this value."
        ),
        timestamp_end: int | None = Query(
            None, description="will limit the data returned to this value."
        ),
        block_ini: int | None = Query(
            None, description="will limit the data returned from this value."
        ),
        block_end: int | None = Query(
            None, description="will limit the data returned to this value."
        ),
        include_operations: bool = Query(
            False,
            description="will include a justifying operations list",
        ),
    ):
        """Returns shares for a given user address"""

        user_address = filter_addresses(user_address)
        hypervisor_address = filter_addresses(hypervisor_address)

        return await get_user_shares(
            user_address=user_address,
            chain=self.chain,
            timestamp_ini=timestamp_ini,
            timestamp_end=timestamp_end,
            block_ini=block_ini,
            block_end=block_end,
            hypervisor_address=hypervisor_address,
            include_operations=include_operations,
        )
