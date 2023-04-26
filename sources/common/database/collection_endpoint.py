import logging
import asyncio
import sys
from datetime import datetime

from sources.subgraph.bins.enums import Chain, Protocol

from sources.common.database.common.collections_common import db_collections_common

logger = logging.getLogger(__name__)


# web3 database related classes


class database_global(db_collections_common):
    """global database helper
    "blocks":
        item-> {id: <network>_<block_number>
                network:
                block:
                timestamp:
                }
    "usd_prices":
        item-> {id: <network>_<block_number>_<address>
                network:
                block:
                address:
                price:
                }
    """

    def __init__(
        self, mongo_url: str, db_name: str = "global", db_collections: dict = None
    ):
        if db_collections is None:
            db_collections = {"blocks": {"id": True}, "usd_prices": {"id": True}}
        super().__init__(
            mongo_url=mongo_url, db_name=db_name, db_collections=db_collections
        )

    async def set_price_usd(
        self, network: str, block: int, token_address: str, price_usd: float
    ):
        data = {
            "id": f"{network}_{block}_{token_address}",
            "network": network,
            "block": int(block),
            "address": token_address,
            "price": float(price_usd),
        }

        await self.save_item_to_database(data=data, collection_name="usd_prices")

    async def set_block(self, network: str, block: int, timestamp: datetime.timestamp):
        data = {
            "id": f"{network}_{block}",
            "network": network,
            "block": block,
            "timestamp": timestamp,
        }
        await self.save_item_to_database(data=data, collection_name="blocks")

    async def get_unique_prices_addressBlock(self, network: str) -> list:
        """get addresses and blocks already present in database
            with price greater than zero.

        Args:
            network (str):

        Returns:
            list:
        """
        return await self.get_items_from_database(
            collection_name="usd_prices", find={"network": network, "price": {"$gt": 0}}
        )

    async def get_price_usd(
        self,
        network: str,
        block: int,
        address: str,
    ) -> list[dict]:
        """get usd price from block

        Args:
            network (str): ethereum, optimism, polygon....
            block (int): number
            address (str): token address

        Returns:
            list[dict]: list of price dict obj
        """
        return await self.get_items_from_database(
            collection_name="usd_prices",
            find={"network": network, "block": block, "address": address},
        )

    async def get_price_usd_closestBlock(
        self,
        network: str,
        block: int,
        address: str,
    ) -> dict:
        """get usd price from closest block to <block>

        Args:
            network (str):
            block (int): number
            address (str): token address

        Returns:
            dict:
        """
        return await self.query_items_from_database(
            query=self.query_blocks_closest(network=network, block=block),
            collection_name="usd_prices",
        )

    async def get_timestamp(
        self,
        network: str,
        block: int,
    ) -> dict:
        return await self.get_items_from_database(
            collection_name="blocks",
            find={"network": network, "block": block},
        )

    async def get_closest_timestamp(self, network: str, block: int) -> dict:
        return await self.query_items_from_database(
            query=self.query_blocks_closest(network=network, block=block),
            collection_name="blocks",
        )

    async def get_block(
        self,
        network: str,
        timestamp: int,
    ) -> dict:
        return await self.get_items_from_database(
            collection_name="blocks", find={"network": network, "timestamp": timestamp}
        )

    async def get_closest_block(self, network: str, timestamp: int) -> dict:
        return await self.query_items_from_database(
            query=self.query_blocks_closest(network=network, timestamp=timestamp),
            collection_name="blocks",
        )

    async def get_all_block_timestamp(self, network: str) -> list:
        """get all blocks and timestamps from database
            sorted by block
        Args:
            network (str):

        Returns:
            list: of sorted blocks timestamps
        """
        return await self.get_items_from_database(
            collection_name="blocks", find={"network": network}, sort=[("block", 1)]
        )

    @staticmethod
    def query_prices_addressBlocks(network: str) -> list[dict]:
        """get addresses and blocks of usd prices present at database and greater than zero

        Args:
            network (str):

        Returns:
            list[dict]:
        """
        return [
            {"$match": {"network": network, "price": {"$gt": 0}}},
        ]

    @staticmethod
    def query_blocks_closest(
        network: str, block: int = 0, timestamp: int = 0
    ) -> list[dict]:
        """find closest block/timestamp item in database

        Args:
            network (str):
            block (int, optional): . Defaults to 0.
            timestamp (int, optional): . Defaults to 0.

        Raises:
            NotImplementedError: when no block or timestamp are provided

        Returns:
            list[dict]:
        """
        if block != 0:
            _search = [block, "$block"]
        elif timestamp != 0:
            _search = [timestamp, "$timestamp"]
        else:
            raise NotImplementedError(
                " provide either block or timestamp. If both are provided, block will be used "
            )
        return [
            {"$match": {"network": network}},
            # Project a diff field that's the absolute difference along with the original doc.
            {
                "$project": {
                    "diff": {"$abs": {"$subtract": _search}},
                    "doc": "$$ROOT",
                }
            },
            # Order the docs by diff
            {"$sort": {"diff": 1}},
            # Take the first one
            {"$limit": 1},
        ]


class database_local(db_collections_common):
    """local database helper
    "static":
        item-> {id: <hypervisor_address>_
                "address": "",  # hypervisor id
                "created": None,  # datetime
                "fee": 0,  # 500
                "network": "",  # polygon
                "name": "",  # xWMATIC-USDC05
                "pool_id": "",  # pool id
                "tokens": [  db_objec_model.token... ],

    "operations":
        item-> {id: <logIndex>_<transactionHash>
                {
                    "_id" : ObjectId("63e0f19e2309ec2395434e4b"),
                    "transactionHash" : "0x8bf414df76a612ce2110cabec4fcaefd9cfc6aaeddd29d7850ac6fa2786adbb4",
                    "blockHash" : "0x286390969e2ddfa3aed6ed885c793bc78bb1974ec7f019116bed6b3edd5fa294",
                    "blockNumber" : 12590365,
                    "address" : "0x9a98bffabc0abf291d6811c034e239e916bbcec0",
                    "timestamp" : 1623108400,
                    "decimals_token0" : 18,
                    "decimals_token1" : 6,
                    "decimals_contract" : 18,
                    "tick" : -197716,
                    "totalAmount0" : "3246736264521404428",
                    "totalAmount1" : "6762363410",
                    "qtty_token0" : "3741331192922089",
                    "qtty_token1" : "0",
                    "topic" : "rebalance",
                    "logIndex" : 118,
                    "id" : "118_0x8bf414df76a612ce2110cabec4fcaefd9cfc6aaeddd29d7850ac6fa2786adbb4"
                }
                ...
                }

    "status":
        item-> {id: <hypervisor address>_<block_number>
                network:
                block:
                address:
                qtty_token0: 0,  # token qtty   (this is tvl = deployed_qtty + owed fees + parked_qtty )
                qtty_token1: 0,  #
                deployed_token0: 0,  # tokens deployed into pool
                deployed_token1: 0,  #
                parked_token0: 0,  # tokens sitting in hype contract ( sleeping )
                parked_token1: 0,  #
                supply: 0,  # total Suply

                }

    "user_status":
        item-> {id: <wallet_address>_<block_number>
                network:
                block:
                address:  <wallet_address>
                ...

                }
    """

    def __init__(self, mongo_url: str, db_name: str, db_collections: dict = None):
        if db_collections is None:
            db_collections = {
                "static": {"id": True},
                "operations": {
                    "id": True,
                    "address": False,
                    "blockNumber": False,
                },
                "status": {
                    "id": True,
                    "address": False,
                    "block": False,
                    "timestamp": False,
                },
                "user_status": {
                    "id": True,
                    "address": False,
                    "hypervisor_address": False,
                    "block": False,
                    "timestamp": False,
                },
                "rewards_static": {"id": True, "address": False},
            }

        super().__init__(
            mongo_url=mongo_url, db_name=db_name, db_collections=db_collections
        )

    # static

    async def set_static(self, data: dict):
        data["id"] = data["address"]
        await self.save_item_to_database(data=data, collection_name="static")

    async def get_unique_tokens(self) -> list:
        """Get a unique token list from static database

        Returns:
            list:
        """
        return await self.get_items_from_database(
            collection_name="static", aggregate=self.query_unique_token_addresses()
        )

    async def get_mostUsed_tokens1(self, limit: int = 10) -> list:
        """Return the addresses of the top used tokens1, present in static database

        Args:
            limit (int, optional): . Defaults to 5.

        Returns:
            list: of {"token":<address>}
        """
        return await self.get_items_from_database(
            collection_name="static",
            aggregate=self.query_status_mostUsed_token1(limit=limit),
        )

    # operation

    async def set_operation(self, data: dict):
        await self.replace_item_to_database(data=data, collection_name="operations")

    async def get_all_operations(self, hypervisor_address: str) -> list:
        """find all hypervisor operations from db
            sort by lowest block and lowest logIndex first

        Args:
            hypervisor_address (str): address

        Returns:
            list: hypervisor status list
        """
        find = {"address": hypervisor_address}
        sort = [("blockNumber", 1), ("logIndex", 1)]
        return await self.get_items_from_database(
            collection_name="operations", find=find, sort=sort
        )

    async def get_hypervisor_operations(
        self,
        hypervisor_address: str,
        timestamp_ini: int | None = None,
        timestamp_end: int | None = None,
        block_ini: int | None = None,
        block_end: int | None = None,
    ) -> list:
        return await self.query_items_from_database(
            collection_name="operations",
            query=self.query_operations(
                hypervisor_address=hypervisor_address,
                timestamp_ini=timestamp_ini,
                timestamp_end=timestamp_end,
                block_ini=block_ini,
                block_end=block_end,
            ),
        )

    async def get_unique_operations_addressBlock(self, topics: list = None) -> list:
        """Retrieve a list of unique blocks + hypervisor addresses present in operations collection

        Returns:
            list: of  {
                    "address" : "0x407e99b20d61f245426031df872966953909e9d3",
                    "block" : 12736656
                    }
        """
        query = []
        if topics:
            query.append({"$match": {"topics": {"$in": topics}}})

        query.extend(
            (
                {
                    "$group": {
                        "_id": {"address": "$address", "block": "$blockNumber"},
                    }
                },
                {
                    "$project": {
                        "address": "$_id.address",
                        "block": "$_id.block",
                    }
                },
                {"$unset": ["_id"]},
            )
        )

        debug_query = f"{query}"

        return await self.get_items_from_database(
            collection_name="operations", aggregate=query
        )

    async def get_user_operations(
        self, user_address: str, timestamp_ini: int | None, timestamp_end: int | None
    ) -> list:
        find = {
            "$or": [
                {"src": user_address},
                {"dst": user_address},
                {"from": user_address},
                {"to": user_address},
            ]
        }

        if timestamp_ini and timestamp_end:
            find["$and"] = [
                {"timestamp": {"$lte": timestamp_end}},
                {"timestamp": {"$gte": timestamp_ini}},
            ]
        elif timestamp_ini:
            find["timestamp"] = {"$gte": timestamp_ini}
        elif timestamp_end:
            find["timestamp"] = {"$lte": timestamp_end}

        sort = [("block", 1)]
        return await self.get_items_from_database(
            collection_name="status", find=find, sort=sort
        )

    # status

    async def set_status(self, data: dict):
        # define database id
        data["id"] = f"{data['address']}_{data['block']}"
        await self.save_item_to_database(data=data, collection_name="status")

    async def get_all_status(self, hypervisor_address: str) -> list:
        """find all hypervisor status from db
            sort by lowest block first

        Args:
            hypervisor_address (str): address

        Returns:
            list: hypervisor status list
        """
        find = {"address": hypervisor_address}
        sort = [("block", 1)]
        return await self.get_items_from_database(
            collection_name="status", find=find, sort=sort
        )

    async def get_hype_status_btwn_blocks(
        self,
        hypervisor_address: str,
        block_ini: int,
        block_end: int,
    ) -> list:
        return await self.query_items_from_database(
            collection_name="status",
            query=self.query_status_btwn_blocks(
                hypervisor_address=hypervisor_address,
                block_ini=block_ini,
                block_end=block_end,
            ),
        )

    async def get_hype_status_blocks(
        self, hypervisor_address: str, blocks: list
    ) -> list:
        find = {"address": hypervisor_address, "block": {"$in": blocks}}
        sort = [("block", 1)]
        return await self.get_items_from_database(
            collection_name="status", find=find, sort=sort
        )

    async def get_unique_status_addressBlock(self) -> list:
        """Retrieve a list of unique blocks + hypervisor addresses present in status collection

        Returns:
            list: of {
                    "address" : "0x407e99b20d61f245426031df872966953909e9d3",
                    "block" : 12736656
                    }
        """
        query = [
            {
                "$group": {
                    "_id": {"address": "$address", "block": "$block"},
                }
            },
            {
                "$project": {
                    "address": "$_id.address",
                    "block": "$_id.block",
                }
            },
            {"$unset": ["_id"]},
        ]
        return await self.get_items_from_database(
            collection_name="status", aggregate=query
        )

    async def get_status_feeReturn_data(
        self,
        hypervisor_address: str,
        timestamp_ini: int,
        timestamp_end: int,
    ) -> list:
        return await self.query_items_from_database(
            collection_name="status",
            query=self.query_status_feeReturn_data(
                hypervisor_address=hypervisor_address,
                timestamp_ini=timestamp_ini,
                timestamp_end=timestamp_end,
            ),
        )

    async def get_status_feeReturn_data_alternative(
        self,
        hypervisor_address: str,
        timestamp_ini: int,
        timestamp_end: int,
    ) -> list:
        return await self.query_items_from_database(
            collection_name="status",
            query=self.query_status_feeReturn_data_alternative(
                hypervisor_address=hypervisor_address,
                timestamp_ini=timestamp_ini,
                timestamp_end=timestamp_end,
            ),
        )

    # user status

    async def set_user_status(self, data: dict):
        """

        Args:
            data (dict):
        """
        # define database id
        data[
            "id"
        ] = f"{data['address']}_{data['block']}_{data['logIndex']}_{data['hypervisor_address']}"

        # convert decimal to bson compatible and save
        await self.replace_item_to_database(data=data, collection_name="user_status")

    async def get_user_status(
        self, address: str, block_ini: int = 0, block_end: int = 0
    ) -> list:
        _main_match = {
            "address": address,
            "topic": {"$in": ["report", "withdraw", "deposit", "transfer"]},
        }
        if block_ini and block_end:
            _main_match["$and"] = [
                {"block": {"$lte": block_end}},
                {"block": {"$gte": block_ini}},
            ]
        elif block_ini:
            _main_match["block"] = {"$gte": block_ini}
        elif block_end:
            _main_match["block"] = {"$lte": block_end}

        query = [
            {"$match": _main_match},
            {"$sort": {"block": 1}},
            {
                "$group": {
                    "_id": "$hypervisor_address",
                    "hypervisor_address": {"$first": "$hypervisor_address"},
                    "history": {
                        "$push": {
                            "hypervisor_address": "$hypervisor_address",
                            "block": "$block",
                            "timestamp": "$timestamp",
                            "topic": "$topic",
                            "investment_qtty_token0": "$investment_qtty_token0",
                            "investment_qtty_token1": "$investment_qtty_token1",
                            "total_investment_qtty_in_usd": "$total_investment_qtty_in_usd",
                            "total_investment_qtty_in_token0": "$total_investment_qtty_in_token0",
                            "total_investment_qtty_in_token1": "$total_investment_qtty_in_token1",
                            "underlying_token0": "$underlying_token0",
                            "underlying_token1": "$underlying_token1",
                            "fees_collected_token0": "$fees_collected_token0",
                            "fees_collected_token1": "$fees_collected_token1",
                            "fees_owed_token0": "$fees_owed_token0",
                            "fees_owed_token1": "$fees_owed_token1",
                            "fees_uncollected_token0": "$fees_uncollected_token0",
                            "fees_uncollected_token1": "$fees_uncollected_token1",
                            "usd_price_token0": "$usd_price_token0",
                            "usd_price_token1": "$usd_price_token1",
                            "divestment_base_qtty_token0": "$divestment_base_qtty_token0",
                            "divestment_base_qtty_token1": "$divestment_base_qtty_token1",
                            "divestment_fee_qtty_token0": "$divestment_fee_qtty_token0",
                            "divestment_fee_qtty_token1": "$divestment_fee_qtty_token1",
                        }
                    },
                }
            },
            {"$unset": ["_id"]},
        ]

        debug_query = f"{query}"
        return [
            self.convert_decimal_to_float(item=self.convert_d128_to_decimal(item=item))
            for item in await self.query_items_from_database(
                query=query, collection_name="user_status"
            )
        ]

    # all

    async def get_items(self, collection_name: str, **kwargs) -> list:
        """Any

        Returns:
            list: of results
        """
        return await self.get_items_from_database(
            collection_name=collection_name, **kwargs
        )

    async def get_max_field(self, collection: str, field: str) -> list:
        """get the maximum field present in db
        Args:
            collection (str): _description_
            field (str): _description_

        Returns:
            list: of { "max": <value>}
        """
        return await self.get_items_from_database(
            collection_name=collection,
            aggregate=self.query_max(field=field),
        )

    # queries

    @staticmethod
    def query_unique_addressBlocks() -> list[dict]:
        """retriev

        Args:
            field (str): ca

        Returns:
            list[dict]: _description_
        """
        # return query
        return [
            {
                "$group": {
                    "_id": {"address": "$address", "block": "$blockNumber"},
                }
            },
            {
                "$project": {
                    "address": "$_id.address",
                    "block": "$_id.block",
                }
            },
            {"$unset": ["_id"]},
        ]

    @staticmethod
    def query_unique_token_addresses() -> list[dict]:
        """Unique token list using status database

        Returns:
            list[dict]:
        """
        return [
            {
                "$group": {
                    "_id": "$pool.address",
                    "items": {"$push": "$$ROOT"},
                }
            },
            {"$project": {"_id": "$_id", "last": {"$last": "$items"}}},
            {
                "$project": {
                    "_id": "$_id",
                    "token": ["$last.pool.token0.address", "$last.pool.token1.address"],
                }
            },
            {"$unwind": "$token"},
            {"$group": {"_id": "$token"}},
        ]

    @staticmethod
    def query_status_btwn_blocks(
        hypervisor_address: str,
        block_ini: datetime.timestamp,
        block_end: datetime.timestamp,
    ) -> list[dict]:
        """get status between blocks"""
        return [
            {
                "$match": {
                    "address": hypervisor_address,
                    "block": {"$gte": block_ini, "$lte": block_end},
                }
            },
            {"$sort": {"block": -1}},
        ]

    @staticmethod
    def query_status_mostUsed_token1(limit: int = 5) -> list[dict]:
        """return the top most used token1 address of static database
            ( may be used in status too)

        Returns:
            list[dict]: _description_
        """
        return [
            {
                "$group": {
                    "_id": {"token1": "$pool.token1.address"},
                    "symbol": {"$last": "$pool.token1.symbol"},
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"count": -1}},
            {"$limit": limit},
            {
                "$project": {
                    "token": "$_id.token1",
                    "symbol": "$symbol",
                }
            },
            {"$unset": ["_id"]},
        ]

    @staticmethod
    def query_max(field: str) -> list[dict]:
        return [
            {
                "$group": {
                    "_id": "id",
                    "max": {"$max": f"${field}"},
                }
            },
            {"$unset": ["_id"]},
        ]

    @staticmethod
    def query_status_feeReturn_data(
        hypervisor_address: str,
        timestamp_ini: int,
        timestamp_end: int,
    ) -> list[dict]:
        """Get data to construct feeAPY APR using equal totalSupply values between blocks to identify APY period.
            This is a prone to error method as totalSupply may sporadically coincide in diff periods ...

        Args:
            hypervisor_address (str):
            timestamp_ini (int):
            timestamp_end (int):

        Returns:
            list[dict]:
                            "period_days" : 0.01653935185185185,
                            "ini_block" : NumberInt(14937408),
                            "end_block" : NumberInt(14937491),
                            "ini_timestamp" : NumberInt(1654849152),
                            "end_timestamp" : NumberInt(1654850581),
                            "ini_supply" : NumberDecimal("91.89181431665138243"),
                            "end_supply" : NumberDecimal("91.89181431665138243"),
                            "ini_tvl0" : NumberDecimal("47083.654951511146650678"),
                            "ini_tvl1" : NumberDecimal("69.395815272326034611"),
                            "ini_fees_uncollected0" : NumberDecimal("6.523979236566497"),
                            "ini_fees_uncollected1" : NumberDecimal("0.01195312935918579"),
                            "ini_fees_owed0" : NumberDecimal("0.0000"),
                            "ini_fees_owed1" : NumberDecimal("0.0000"),
                            "end_tvl0" : NumberDecimal("47083.654951511146650678"),
                            "end_tvl1" : NumberDecimal("69.395815272326034611"),
                            "end_fees_uncollected0" : NumberDecimal("6.523979236566497"),
                            "end_fees_uncollected1" : NumberDecimal("0.01195312935918579"),
                            "end_fees_owed0" : NumberDecimal("0.0000"),
                            "end_fees_owed1" : NumberDecimal("0.0000"),
                            "error_ini" : NumberInt(0),
                            "error_end" : NumberInt(0)
        """
        return [
            {
                "$match": {
                    "address": hypervisor_address,
                    "$and": [
                        {"timestamp": {"$lte": timestamp_end}},
                        {"timestamp": {"$gte": timestamp_ini}},
                    ],
                }
            },
            {"$sort": {"block": 1}},
            {
                "$group": {
                    "_id": "$totalSupply",
                    "items": {"$push": "$$ROOT"},
                    "max_block": {"$max": "$block"},
                    "min_block": {"$min": "$block"},
                    "max_timestamp": {"$max": "$timestamp"},
                    "min_timestamp": {"$min": "$timestamp"},
                }
            },
            {
                "$addFields": {
                    "period_days": {
                        "$divide": [
                            {"$subtract": ["$max_timestamp", "$min_timestamp"]},
                            60 * 60 * 24,
                        ]
                    }
                }
            },
            {"$sort": {"min_block": 1}},
            {
                "$project": {
                    "max_block": "$max_block",
                    "min_block": "$min_block",
                    "max_timestamp": "$max_timestamp",
                    "min_timestamp": "$min_timestamp",
                    "period_days": "$period_days",
                    "ini_snapshot": {"$arrayElemAt": ["$items", 0]},
                    "end_snapshot": {"$arrayElemAt": ["$items", -1]},
                }
            },
            {
                "$project": {
                    "max_block": "$max_block",
                    "min_block": "$min_block",
                    "max_timestamp": "$max_timestamp",
                    "min_timestamp": "$min_timestamp",
                    "period_days": "$period_days",
                    "ini_snapshot": "$ini_snapshot",
                    "end_snapshot": "$end_snapshot",
                    "error_ini": {"$subtract": ["$ini_snapshot.block", "$min_block"]},
                    "error_end": {"$subtract": ["$end_snapshot.block", "$max_block"]},
                }
            },
            {
                "$project": {
                    "period_days": "$period_days",
                    "ini_block": "$ini_snapshot.block",
                    "end_block": "$end_snapshot.block",
                    "ini_timestamp": "$ini_snapshot.timestamp",
                    "end_timestamp": "$end_snapshot.timestamp",
                    "ini_supply": {
                        "$divide": [
                            {"$toDecimal": "$ini_snapshot.totalSupply"},
                            {"$pow": [10, "$ini_snapshot.decimals"]},
                        ]
                    },
                    "end_supply": {
                        "$divide": [
                            {"$toDecimal": "$end_snapshot.totalSupply"},
                            {"$pow": [10, "$end_snapshot.decimals"]},
                        ]
                    },
                    "ini_tvl0": {
                        "$divide": [
                            {"$toDecimal": "$ini_snapshot.totalAmounts.total0"},
                            {"$pow": [10, "$ini_snapshot.pool.token0.decimals"]},
                        ]
                    },
                    "ini_tvl1": {
                        "$divide": [
                            {"$toDecimal": "$ini_snapshot.totalAmounts.total1"},
                            {"$pow": [10, "$ini_snapshot.pool.token1.decimals"]},
                        ]
                    },
                    "ini_fees_uncollected0": {
                        "$divide": [
                            {
                                "$toDecimal": "$ini_snapshot.fees_uncollected.qtty_token0"
                            },
                            {"$pow": [10, "$ini_snapshot.pool.token0.decimals"]},
                        ]
                    },
                    "ini_fees_uncollected1": {
                        "$divide": [
                            {
                                "$toDecimal": "$ini_snapshot.fees_uncollected.qtty_token1"
                            },
                            {"$pow": [10, "$ini_snapshot.pool.token1.decimals"]},
                        ]
                    },
                    "ini_fees_owed0": {
                        "$divide": [
                            {"$toDecimal": "$ini_snapshot.tvl.fees_owed_token0"},
                            {"$pow": [10, "$ini_snapshot.pool.token0.decimals"]},
                        ]
                    },
                    "ini_fees_owed1": {
                        "$divide": [
                            {"$toDecimal": "$ini_snapshot.tvl.fees_owed_token1"},
                            {"$pow": [10, "$ini_snapshot.pool.token1.decimals"]},
                        ]
                    },
                    "end_tvl0": {
                        "$divide": [
                            {"$toDecimal": "$end_snapshot.totalAmounts.total0"},
                            {"$pow": [10, "$end_snapshot.pool.token0.decimals"]},
                        ]
                    },
                    "end_tvl1": {
                        "$divide": [
                            {"$toDecimal": "$end_snapshot.totalAmounts.total1"},
                            {"$pow": [10, "$end_snapshot.pool.token1.decimals"]},
                        ]
                    },
                    "end_fees_uncollected0": {
                        "$divide": [
                            {
                                "$toDecimal": "$end_snapshot.fees_uncollected.qtty_token0"
                            },
                            {"$pow": [10, "$end_snapshot.pool.token0.decimals"]},
                        ]
                    },
                    "end_fees_uncollected1": {
                        "$divide": [
                            {
                                "$toDecimal": "$end_snapshot.fees_uncollected.qtty_token1"
                            },
                            {"$pow": [10, "$end_snapshot.pool.token1.decimals"]},
                        ]
                    },
                    "end_fees_owed0": {
                        "$divide": [
                            {"$toDecimal": "$end_snapshot.tvl.fees_owed_token0"},
                            {"$pow": [10, "$end_snapshot.pool.token0.decimals"]},
                        ]
                    },
                    "end_fees_owed1": {
                        "$divide": [
                            {"$toDecimal": "$end_snapshot.tvl.fees_owed_token1"},
                            {"$pow": [10, "$end_snapshot.pool.token1.decimals"]},
                        ]
                    },
                    "error_ini": "$error_ini",
                    "error_end": "$error_end",
                }
            },
            {"$unset": ["_id"]},
        ]

    @staticmethod
    def query_status_feeReturn_data_alternative(
        hypervisor_address: str, timestamp_ini: int, timestamp_end: int
    ) -> list[dict]:
        """

            old descript: return a list of status ordered by block matching deposit,withdraw,rebalance and zeroBurn operation blocks and those same blocks -1
            Each status has a order field indicating if this is the initial period status with a "first" value
            or this is the end of the perios status with the "last" value

        Args:
            hypervisor_address (str):
            timestamp_ini (int):
            timestamp_end (int):

        Returns:
            list[dict]:   Each status has an <order> field indicating if this is the initial period status with a "first" value
            or this is the end of the perios status with the "last" value
        """

        return [
            {
                "$match": {
                    "address": hypervisor_address,
                    "$and": [
                        {"timestamp": {"$lte": timestamp_end}},
                        {"timestamp": {"$gte": timestamp_ini}},
                    ],
                }
            },
            {"$sort": {"block": 1}},
            {
                "$group": {
                    "_id": "$totalSupply",
                    "items": {"$push": "$$ROOT"},
                    "max_block": {"$max": "$block"},
                    "min_block": {"$min": "$block"},
                    "max_timestamp": {"$max": "$timestamp"},
                    "min_timestamp": {"$min": "$timestamp"},
                }
            },
            {
                "$addFields": {
                    "period_days": {
                        "$divide": [
                            {"$subtract": ["$max_timestamp", "$min_timestamp"]},
                            60 * 60 * 24,
                        ]
                    }
                }
            },
            {"$sort": {"min_block": 1}},
            {"$unwind": "$items"},
            {"$replaceRoot": {"newRoot": "$items"}},
            {"$unset": ["_id", "id"]},
        ]

    @staticmethod
    def query_all_users(
        user_address: str, timestamp_ini: int = None, timestamp_end: int = None
    ) -> list[dict]:
        _match = {
            "$or": [
                {"src": user_address},
                {"dst": user_address},
                {"from": user_address},
                {"to": user_address},
            ]
        }

        if timestamp_ini and timestamp_end:
            _match["$and"] = [
                {"timestamp": {"$lte": timestamp_end}},
                {"timestamp": {"$gte": timestamp_ini}},
            ]
        elif timestamp_ini:
            _match["timestamp"] = {"$gte": timestamp_ini}
        elif timestamp_end:
            _match["timestamp"] = {"$lte": timestamp_end}

        return [{"$match": _match}, {"$sort": {"timestamp": 1}}]

    @staticmethod
    def query_uncollected_fees(
        hypervisor_address: str | None = None,
        timestamp: int | None = None,
        block: int | None = None,
    ) -> list[dict]:
        """If no hypervisor_address is provided, it will return all uncollected fees

        Args:
            hypervisor_address (str | None, optional): . Defaults to None.
            timestamp (int):  lower than or equeal to this timestamp. Defaults to None.
            block (int):  lower than or equal to this block. Defaults to None.

        Returns:
            list[dict]: {
             "address":
            "symbol":
            "block":
            "timestamp":
            "uncollectedFees0":
            "uncollectedFees1":
            "owedFees0":
            "owedFees1"
            "totalFees0"
            "totalFees1"
            }
        """
        query = [
            {"$sort": {"block": -1}},
            {
                "$group": {
                    "_id": "$address",
                    "item": {"$first": "$$ROOT"},
                }
            },
            {
                "$project": {
                    "address": "$_id",
                    "symbol": "$item.symbol",
                    "block": "$item.block",
                    "timestamp": "$item.timestamp",
                    "uncollectedFees0": {
                        "$divide": [
                            {"$toDecimal": "$item.fees_uncollected.qtty_token0"},
                            {"$pow": [10, "$item.pool.token0.decimals"]},
                        ]
                    },
                    "uncollectedFees1": {
                        "$divide": [
                            {"$toDecimal": "$item.fees_uncollected.qtty_token1"},
                            {"$pow": [10, "$item.pool.token1.decimals"]},
                        ]
                    },
                    "owedFees0": {
                        "$divide": [
                            {"$toDecimal": "$item.tvl.fees_owed_token0"},
                            {"$pow": [10, "$item.pool.token0.decimals"]},
                        ]
                    },
                    "owedFees1": {
                        "$divide": [
                            {"$toDecimal": "$item.tvl.fees_owed_token1"},
                            {"$pow": [10, "$item.pool.token1.decimals"]},
                        ]
                    },
                }
            },
            {
                "$addFields": {
                    "totalFees0": {"$sum": ["uncollectedFees0", "$owedFees0"]},
                    "totalFees1": {"$sum": ["uncollectedFees1", "$owedFees1"]},
                }
            },
            {"$unset": ["_id"]},
        ]

        if hypervisor_address:
            if block:
                query.insert(
                    0,
                    {
                        "$match": {
                            "address": hypervisor_address,
                            "block": {"$lte": block},
                        }
                    },
                )
            elif timestamp:
                query.insert(
                    0,
                    {
                        "$match": {
                            "address": hypervisor_address,
                            "block": {"$lte": block},
                        }
                    },
                )
            else:
                query.insert(0, {"$match": {"address": hypervisor_address}})

        return query

    @staticmethod
    def query_operations(
        hypervisor_address: str,
        timestamp_ini: int | None = None,
        timestamp_end: int | None = None,
        block_ini: int | None = None,
        block_end: int | None = None,
    ) -> list[dict]:
        """get operations

            Supply end timestamp or block to get operations until that point in time
            Supply both timestamp and block to get operations between those points in time

        Args:
            timestamp_ini (int): initial timestamp
            timestamp_end (int): end timestamp
            block_ini (int): initial block
            block_end (int): end block

        Returns:
            list[dict]:
        """
        _match = {
            "address": hypervisor_address,
            "timestamp": {"$gte": timestamp_ini, "$lte": timestamp_end},
        }
        if block_ini and block_end:
            _match["blockNumber"] = {"$gte": block_ini, "$lte": block_end}
        elif block_ini:
            _match["blockNumber"] = {"$gte": block_ini}
        elif block_end:
            _match["blockNumber"] = {"$lte": block_end}
        elif timestamp_ini and timestamp_end:
            _match["timestamp"] = {"$gte": timestamp_ini, "$lte": timestamp_end}
        elif timestamp_ini:
            _match["timestamp"] = {"$gte": timestamp_ini}
        elif timestamp_end:
            _match["timestamp"] = {"$lte": timestamp_end}

        return [
            {"$match": _match},
            {"$sort": {"blockNumber": -1, "logIndex": 1}},
        ]

    @staticmethod
    def query_operations_summary(
        hypervisor_address: str,
        timestamp_ini: int | None = None,
        timestamp_end: int | None = None,
        block_ini: int | None = None,
        block_end: int | None = None,
    ) -> list[dict]:
        """_summary_

        Args:
            hypervisor_address (str): _description_
            timestamp_ini (int | None, optional): _description_. Defaults to None.
            timestamp_end (int | None, optional): _description_. Defaults to None.
            block_ini (int | None, optional): _description_. Defaults to None.
            block_end (int | None, optional): _description_. Defaults to None.

        Returns:
            list[dict]: {
                            "_id" : "0x7f92463e24b2ea1f7267aceed3ad68f7a956d2d8",
                            "address" : "0x7f92463e24b2ea1f7267aceed3ad68f7a956d2d8",
                            "block_ini" : NumberInt(13591510),
                            "block_end" : NumberInt(15785095),
                            "timestamp_ini" : NumberInt(1636587581),
                            "timestamp_end" : NumberInt(1666218047),
                            "deposits_token0" : NumberDecimal("95.257532479790920"),
                            "deposits_token1" : NumberDecimal("8.1970"),
                            "withdraws_token0" : NumberDecimal("84.270732146125590648"),
                            "withdraws_token1" : NumberDecimal("1.619807984061774091"),
                            "collectedFees_token0" : NumberDecimal("27.464269473110871371"),
                            "collectedFees_token1" : NumberDecimal("1.388745951660998281"),
                            "zeroBurnFees_token0" : NumberDecimal("0.0000"),
                            "zeroBurnFees_token1" : NumberDecimal("0.0000")
                        }

        """
        query = [
            {
                "$project": {
                    "address": "$address",
                    "block": "$blockNumber",
                    "timestamp": "$timestamp",
                    "deposits_token0": {
                        "$cond": [
                            {"$eq": ["$topic", "deposit"]},
                            {"$toDecimal": "$qtty_token0"},
                            0,
                        ]
                    },
                    "deposits_token1": {
                        "$cond": [
                            {"$eq": ["$topic", "deposit"]},
                            {"$toDecimal": "$qtty_token1"},
                            0,
                        ]
                    },
                    "withdraws_token0": {
                        "$cond": [
                            {"$eq": ["$topic", "withdraw"]},
                            {"$toDecimal": "$qtty_token0"},
                            0,
                        ]
                    },
                    "withdraws_token1": {
                        "$cond": [
                            {"$eq": ["$topic", "withdraw"]},
                            {"$toDecimal": "$qtty_token1"},
                            0,
                        ]
                    },
                    "collectedFees_token0": {
                        "$cond": [
                            {"$eq": ["$topic", "rebalance"]},
                            {"$toDecimal": "$qtty_token0"},
                            0,
                        ]
                    },
                    "collectedFees_token1": {
                        "$cond": [
                            {"$eq": ["$topic", "rebalance"]},
                            {"$toDecimal": "$qtty_token1"},
                            0,
                        ]
                    },
                    "zeroBurnFees_token0": {
                        "$cond": [
                            {"$eq": ["$topic", "zeroBurn"]},
                            {"$toDecimal": "$qtty_token0"},
                            0,
                        ]
                    },
                    "zeroBurnFees_token1": {
                        "$cond": [
                            {"$eq": ["$topic", "zeroBurn"]},
                            {"$toDecimal": "$qtty_token1"},
                            0,
                        ]
                    },
                    "decimals_token0": "$decimals_token0",
                    "decimals_token1": "$decimals_token1",
                }
            },
            {
                "$group": {
                    "_id": "$address",
                    "address": {"$first": "$address"},
                    "block_ini": {"$first": "$block"},
                    "block_end": {"$last": "$block"},
                    "timestamp_ini": {"$first": "$timestamp"},
                    "timestamp_end": {"$last": "$timestamp"},
                    "deposits_token0": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$deposits_token0"},
                                {"$pow": [10, "$decimals_token0"]},
                            ]
                        }
                    },
                    "deposits_token1": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$deposits_token1"},
                                {"$pow": [10, "$decimals_token1"]},
                            ]
                        }
                    },
                    "withdraws_token0": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$withdraws_token0"},
                                {"$pow": [10, "$decimals_token0"]},
                            ]
                        }
                    },
                    "withdraws_token1": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$withdraws_token1"},
                                {"$pow": [10, "$decimals_token1"]},
                            ]
                        }
                    },
                    "collectedFees_token0": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$collectedFees_token0"},
                                {"$pow": [10, "$decimals_token0"]},
                            ]
                        }
                    },
                    "collectedFees_token1": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$collectedFees_token1"},
                                {"$pow": [10, "$decimals_token1"]},
                            ]
                        }
                    },
                    "zeroBurnFees_token0": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$zeroBurnFees_token0"},
                                {"$pow": [10, "$decimals_token0"]},
                            ]
                        }
                    },
                    "zeroBurnFees_token1": {
                        "$sum": {
                            "$divide": [
                                {"$toDecimal": "$zeroBurnFees_token1"},
                                {"$pow": [10, "$decimals_token1"]},
                            ]
                        }
                    },
                }
            },
            {"$unset": ["_id"]},
        ]
        if block_ini and block_end:
            query.insert(0, {"$match": {"$gte": block_ini, "$lte": block_end}})
        elif block_ini:
            query.insert(0, {"$match": {"$gte": block_ini}})
        elif block_end:
            query.insert(0, {"$match": {"$lte": block_end}})
        elif timestamp_ini and timestamp_end:
            query.insert(0, {"$match": {"$gte": timestamp_ini, "$lte": timestamp_end}})
        elif timestamp_ini:
            query.insert(0, {"$match": {"$gte": timestamp_ini}})
        elif timestamp_end:
            query.insert(0, {"$match": {"$lte": timestamp_end}})

        return query
