import logging
import asyncio
import sys
from datetime import datetime

from sources.subgraph.bins.enums import Chain, Protocol
from pymongo import DESCENDING, ASCENDING

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
        self,
        mongo_url: str,
        db_name: str = "global",
        db_collections: dict | None = None,
    ):
        if db_collections is None:
            db_collections = {
                "blocks": {
                    "mono_indexes": {"id": True, "network": False, "block": False},
                    "multi_indexes": [],
                },
                "usd_prices": {
                    "mono_indexes": {
                        "id": True,
                        "address": False,
                        "block": False,
                        "network": False,
                    },
                    "multi_indexes": [
                        [
                            ("address", ASCENDING),
                            ("block", ASCENDING),
                            ("network", ASCENDING),
                        ],
                    ],
                },
                "current_usd_prices": {
                    "mono_indexes": {"id": True, "address": False},
                    "multi_indexes": [
                        [
                            ("address", ASCENDING),
                            ("network", ASCENDING),
                        ],
                    ],
                },
                "configuration": {
                    "mono_indexes": {"id": True},
                    "multi_indexes": [],
                },
                "frontend": {
                    "mono_indexes": {
                        "id": True,
                        "timestamp": False,
                        "protocol": False,
                        "chain": False,
                        "frontend_type": False,
                    },
                    "multi_indexes": [
                        [
                            ("chain", ASCENDING),
                            ("protocol", ASCENDING),
                            ("timestamp", ASCENDING),
                        ],
                    ],
                },
                "reports": {
                    "mono_indexes": {"id": True},
                    "multi_indexes": [],
                },
            }
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
            find={"id": f"{network}_{block}_{address}"},
        )

    async def get_prices_usd_last(
        self, network: str, limit: int | None = 10000
    ) -> list[dict]:
        """get last block known prices of all tokens present in the database"""
        return await self.get_items_from_database(
            collection_name="usd_prices",
            aggregate=self.query_last_prices(network=network, limit=limit),
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

    @staticmethod
    def query_last_prices(network: str, limit: int | None = 10000) -> list[dict]:
        """get last prices from database

        Args:
            network (str):

        Returns:
            list[dict]:
        """
        query = [
            {"$match": {"network": network}},
            {"$sort": {"block": -1}},
        ]
        if limit:
            query.append({"$limit": limit})

        query.append({"$group": {"_id": "$address", "doc": {"$first": "$$ROOT"}}})
        query.append({"$replaceRoot": {"newRoot": "$doc"}})
        return query


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

    def __init__(
        self, mongo_url: str, db_name: str, db_collections: dict | None = None
    ):
        if db_collections is None:
            db_collections = {
                "static": {
                    "mono_indexes": {"id": True, "address": False},
                    "multi_indexes": [],
                },
                "operations": {
                    "mono_indexes": {
                        "id": True,
                        "blockNumber": False,
                        "address": False,
                        "timestamp": False,
                        "topic": False,
                    },
                    "multi_indexes": [
                        [("blockNumber", ASCENDING), ("logIndex", ASCENDING)],
                        [
                            ("dst", ASCENDING),
                            ("src", ASCENDING),
                            ("sender", ASCENDING),
                            ("to", ASCENDING),
                        ],
                    ],
                },
                # hypervisor snapshots
                "status": {
                    "mono_indexes": {
                        "id": True,
                        "block": False,
                        "address": False,
                        "timestamp": False,
                    },
                    "multi_indexes": [],
                },
                "user_operations": {
                    "mono_indexes": {
                        "id": True,
                        "block": False,
                        "user_address": False,
                        "hypervisor_address": False,
                        "timestamp": False,
                        "topic": False,
                        "transactionHash": False,
                        "logIndex": False,
                        "customIndex": False,
                    },
                    "multi_indexes": [
                        [("block", DESCENDING), ("logIndex", DESCENDING)],
                        [
                            ("transactionHash", DESCENDING),
                            ("logIndex", DESCENDING),
                            ("customIndex", DESCENDING),
                        ],
                    ],
                },
                "rewards_static": {
                    "mono_indexes": {
                        "id": True,
                        "hypervisor_address": False,
                        "rewarder_address": False,
                    },
                    "multi_indexes": [],
                },
                "rewards_status": {
                    "mono_indexes": {
                        "id": True,
                        "hypervisor_address": False,
                        "rewarder_address": False,
                        "block": False,
                        "timestamp": False,
                    },
                    "multi_indexes": [],
                },
                "queue": {
                    "mono_indexes": {
                        "id": True,
                        "type": False,
                    },
                    "multi_indexes": [
                        [
                            ("count", ASCENDING),
                            ("processing", ASCENDING),
                            ("creation", ASCENDING),
                        ],
                    ],
                },
                "hypervisor_returns": {
                    "mono_indexes": {
                        "id": True,
                        "address": False,
                        "ini_block": False,
                        "end_block": False,
                        "ini_timestamp": False,
                        "end_timestamp": False,
                    },
                    "multi_indexes": [],
                },
                "latest_multifeedistribution": {
                    "mono_indexes": {
                        "id": True,
                        "block": False,
                        "timestamp": False,
                        "address": False,
                    },
                    "multi_indexes": [],
                },
                "reports": {
                    "mono_indexes": {
                        "id": True,
                        "type": False,
                    },
                    "multi_indexes": [],
                },
                "revenue_operations": {
                    "mono_indexes": {
                        "id": True,
                        "blockNumber": False,
                        "address": False,
                        "timestamp": False,
                    },
                    "multi_indexes": [
                        [("blockNumber", ASCENDING), ("logIndex", ASCENDING)],
                    ],
                },
                # latest hypervisor snapshots
                "latest_hypervisor_snapshots": {
                    "mono_indexes": {
                        "id": True,
                        "block": False,
                        "address": True,
                        "timestamp": False,
                    },
                    "multi_indexes": [],
                },
                "latest_reward_snapshots": {
                    "mono_indexes": {
                        "id": True,
                        "block": False,
                        "address": False,
                        "timestamp": False,
                    },
                    "multi_indexes": [],
                },
                "latest_hypervisor_returns": {
                    "mono_indexes": {
                        "id": True,
                        "address": False,
                        "ini_block": False,
                        "end_block": False,
                        "ini_timestamp": False,
                        "end_timestamp": False,
                    },
                    "multi_indexes": [],
                },
                "transaction_receipts": {
                    "mono_indexes": {
                        "id": True,
                        "block": False,
                        "timestamp": False,
                    },
                    "multi_indexes": [],
                },
            }

        super().__init__(
            mongo_url=mongo_url, db_name=db_name, db_collections=db_collections
        )

    # static

    async def set_static(self, data: dict):
        data["id"] = data["address"]
        await self.save_item_to_database(data=data, collection_name="static")

    async def get_gamma_service_fees(self) -> dict:
        """Get the agreed service fee (%) to be collected by the gamma protocol for each hypervisor

        Returns:
            dict: {"hypervisor address":  {"symbol":<symbol>, "dex":<dex> ,"fee":<fee>} }
        """
        # some hypes have the pool fee at gamma fees field: those are 1/10 hardcoded
        return {
            x["address"]: {
                x["symbol"],
                x["dex"],
                (1 / x["fee"]) if x["fee"] < 100 else 1 / 10,
            }
            for x in await self.get_items_from_database(
                collection_name="static", projection={"address", "symbol", "dex", "fee"}
            )
        }

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

    async def get_user_operations_status(
        self, user_address: str, timestamp_ini: int | None, timestamp_end: int | None
    ) -> list:
        """retrieve all user operations and their status, grouped by hypervisor

        Args:
            user_address (str):
            timestamp_ini (int | None):
            timestamp_end (int | None):

        Returns:
            list: of hypervisors with their operations and status
        """

        _match = {
            "$or": [
                {"sender": user_address.lower()},
                {"to": user_address.lower()},
                {"src": user_address.lower()},
                {"dst": user_address.lower()},
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

        query = [
            {"$match": _match},
            {
                "$lookup": {
                    "from": "status",
                    "let": {
                        "operation_hype_address": "$address",
                        "operation_block": "$blockNumber",
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {
                                            "$eq": [
                                                "$address",
                                                "$$operation_hype_address",
                                            ]
                                        },
                                        {"$eq": ["$block", "$$operation_block"]},
                                    ],
                                }
                            }
                        },
                        {"$unset": ["_id"]},
                    ],
                    "as": "status",
                }
            },
            {"$unset": ["_id"]},
            {"$unwind": "$status"},
            {"$sort": {"block": 1}},
            {
                "$group": {
                    "_id": "$address",
                    "hypervisor_address": {"$first": "$address"},
                    "operations": {"$push": "$$ROOT"},
                }
            },
            {"$unset": ["_id"]},
        ]

        debug_query = f"{query}"
        return await self.query_items_from_database(
            collection_name="operations", query=query
        )

    async def get_grouped_user_current_status(
        self, user_address: str, chain: Chain
    ) -> list:
        """Get a grouped by hypervisor picture of a user, with regards shares and operations.

        Args:
            user_address (str): user address

        Returns:
            list: of hypervisors with their operations and status like:

            {
            "hypervisor": "0x998c07827578c83949a6b755dd3416fdfd98a75e",
            "last_block": 146654091,
            "last_timestamp": 1699000302,
            "decimals": {
                "hype": 18,
                "token0": 18,
                "token1": 18
            },
            "last_shares": {
                "$numberDecimal": "2454851542501644905"
            },
            "last_token0": {
                "$numberDecimal": "161005324767134413"
            },
            "last_token1": {
                "$numberDecimal": "3675185849966923774"
            },
            "operations": [
                {
                "block": 146654091,
                "timestamp": 1699000302,
                "topic": "deposit",
                "shares": {
                    "$numberDecimal": "2454851542501644905"
                },
                "token0": {
                    "$numberDecimal": "161005324767134413"
                },
                "token1": {
                    "$numberDecimal": "3675185849966923774"
                }
                }
            ]
            }

        """
        return await self.get_items_from_database(
            collection_name="operations",
            aggregate=self.query_user_shares_operations(
                user_address=user_address.lower(), chain=chain
            ),
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
        data["id"] = (
            f"{data['address']}_{data['block']}_{data['logIndex']}_{data['hypervisor_address']}"
        )

        # convert decimal to bson compatible and save
        await self.replace_item_to_database(data=data, collection_name="user_status")

    async def get_user_status(
        self, address: str, block_ini: int = 0, block_end: int = 0
    ) -> list:
        _match = {
            "user_address": address,
        }
        if block_ini and block_end:
            _match["$and"] = [
                {"block": {"$lte": block_end}},
                {"block": {"$gte": block_ini}},
            ]
        elif block_ini:
            _match["block"] = {"$gte": block_ini}
        elif block_end:
            _match["block"] = {"$lte": block_end}

        query = [
            {"$match": _match},
            {
                "$sort": {"block": 1},
            },
            {
                "$addFields": {
                    "shares": {"$subtract": ["$shares_in", "$shares_out"]},
                    "fees_usd": {
                        "$sum": [
                            {"$multiply": ["$price_usd_token0", "$fees_token0_in"]},
                            {"$multiply": ["$price_usd_token1", "$fees_token1_in"]},
                        ]
                    },
                    "shares_buy": {"$multiply": ["$price_usd_share", "$shares_in"]},
                    "shares_sell": {"$multiply": ["$price_usd_share", "$shares_out"]},
                }
            },
            {
                "$group": {
                    "_id": "$hypervisor_address",
                    "shares": {"$sum": "$shares"},
                    "first_prices": {
                        "$first": {
                            "underlying_token0_per_share": "$underlying_token0_per_share",
                            "underlying_token1_per_share": "$underlying_token1_per_share",
                            "price_usd_token0": "$price_usd_token0",
                            "price_usd_token1": "$price_usd_token1",
                        }
                    },
                    "last_prices": {
                        "$last": {
                            "underlying_token0_per_share": "$underlying_token0_per_share",
                            "underlying_token1_per_share": "$underlying_token1_per_share",
                            "price_usd_token0": "$price_usd_token0",
                            "price_usd_token1": "$price_usd_token1",
                        }
                    },
                    "fees_usd": {"$sum": "$fees_usd"},
                    "fees_token0": {"$sum": "$fees_token0_in"},
                    "fees_token1": {"$sum": "$fees_token1_in"},
                    "first_timestamp": {"$first": "$timestamp"},
                    "last_timestamp": {"$last": "$timestamp"},
                    "operations": {
                        "$push": {
                            "block": "$block",
                            "timestamp": "$timestamp",
                            "operation": "$topic",
                            "shares_move": "$shares",
                            "fees_token0_move": "$fees_token0_in",
                            "fees_token1_move": "$fees_token1_in",
                            "underlying_token0_move": {
                                "$subtract": ["$token0_in", "$token0_out"]
                            },
                            "underlying_token1_move": {
                                "$subtract": ["$token1_in", "$token1_out"]
                            },
                            "price_usd_share": "$price_usd_share",
                            "price_usd_token0": "$price_usd_token0",
                            "price_usd_token1": "$price_usd_token1",
                            "underlying_token0_per_share": "$underlying_token0_per_share",
                            "underlying_token1_per_share": "$underlying_token1_per_share",
                        }
                    },
                }
            },
            {
                "$project": {
                    "hypervisor_address": "$_id",
                    "period_seconds": {
                        "$subtract": ["$last_timestamp", "$first_timestamp"]
                    },
                    "shares": {
                        "total": "$shares",
                        "last_value_usd": {
                            "$sum": [
                                {
                                    "$multiply": [
                                        "$shares",
                                        "$last_prices.underlying_token0_per_share",
                                        "$last_prices.price_usd_token0",
                                    ]
                                },
                                {
                                    "$multiply": [
                                        "$shares",
                                        "$last_prices.underlying_token1_per_share",
                                        "$last_prices.price_usd_token1",
                                    ]
                                },
                            ]
                        },
                        "first_value_usd": {
                            "$sum": [
                                {
                                    "$multiply": [
                                        "$shares",
                                        "$first_prices.underlying_token0_per_share",
                                        "$first_prices.price_usd_token0",
                                    ]
                                },
                                {
                                    "$multiply": [
                                        "$shares",
                                        "$first_prices.underlying_token1_per_share",
                                        "$first_prices.price_usd_token1",
                                    ]
                                },
                            ]
                        },
                    },
                    "underlying_tokens": {
                        "first_qtty_token0": {
                            "$multiply": [
                                "$shares",
                                "$first_prices.underlying_token0_per_share",
                            ]
                        },
                        "first_qtty_token1": {
                            "$multiply": [
                                "$shares",
                                "$first_prices.underlying_token1_per_share",
                            ]
                        },
                        "last_qtty_token0": {
                            "$multiply": [
                                "$shares",
                                "$last_prices.underlying_token0_per_share",
                            ]
                        },
                        "last_qtty_token1": {
                            "$multiply": [
                                "$shares",
                                "$last_prices.underlying_token1_per_share",
                            ]
                        },
                    },
                    "fees": {
                        "total_token0": "$fees_token0",
                        "total_token1": "$fees_token1",
                        "total_usd": "$fees_usd",
                    },
                    "prices": {
                        "first_prices": "$first_prices",
                        "last_prices": "$last_prices",
                        "price_variation": {
                            "token0_usd": {
                                "$subtract": [
                                    "$last_prices.price_usd_token0",
                                    "$first_prices.price_usd_token0",
                                ]
                            },
                            "token1_usd": {
                                "$subtract": [
                                    "$last_prices.price_usd_token1",
                                    "$first_prices.price_usd_token1",
                                ]
                            },
                        },
                    },
                    "operations": "$operations",
                }
            },
            {
                "$addFields": {
                    "shares.first_value_in_last_prices_usd": {
                        "$sum": [
                            {
                                "$multiply": [
                                    "$underlying_tokens.first_qtty_token0",
                                    "$prices.last_prices.price_usd_token0",
                                ]
                            },
                            {
                                "$multiply": [
                                    "$underlying_tokens.first_qtty_token1",
                                    "$prices.last_prices.price_usd_token1",
                                ]
                            },
                        ]
                    },
                },
            },
            {"$unset": ["_id"]},
        ]

        return [
            self.convert_decimal_to_float(item=self.convert_d128_to_decimal(item=item))
            for item in await self.query_items_from_database(
                query=query, collection_name="user_operations"
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

    # latest
    async def get_latest_multifeedistribution(
        self,
        mfd_addresses: list[str] | None = None,
        hypervisor_addresses: list[str] | None = None,
        dex: str | None = None,
    ) -> list:
        """Get the latest multifeedistributor contract status

        Args:
            mfd_addresses (list[str] | None, optional): multiFeeDistributor contract addresses.
            hypervisor_addresses (list[str] | None, optional): Hypervisor addresses.
            dex (str | None, optional):

        Returns:
            list: of the latest multifeedistributor contract status
        """
        return await self.get_items_from_database(
            collection_name="latest_multifeedistribution",
            aggregate=self.query_latest_multifeedistributor(
                mfd_addresses=mfd_addresses,
                hypervisor_addresses=hypervisor_addresses,
                dex=dex,
            ),
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

        # debug_query = f"{query}"
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
        hypervisor_addresses: list[str] | None = None,
        timestamp_ini: int | None = None,
        timestamp_end: int | None = None,
        block_ini: int | None = None,
        block_end: int | None = None,
    ) -> list[dict]:
        """_summary_

        Args:
            hypervisor_addresses (list[str] | None, optional): list of hypervisor addresses
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
                    "block_ini": {"$min": "$block"},
                    "block_end": {"$max": "$block"},
                    "timestamp_ini": {"$min": "$timestamp"},
                    "timestamp_end": {"$max": "$timestamp"},
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

        # build match
        _and = []
        _match = {}

        # add block or timestamp in query
        if block_ini:
            _and.append({"blockNumber": {"$gte": block_ini}})
        elif timestamp_ini:
            _and.append({"timestamp": {"$gte": timestamp_ini}})
        if block_end:
            _and.append({"blockNumber": {"$lte": block_end}})
        elif timestamp_end:
            _and.append({"timestamp": {"$lte": timestamp_end}})

        # add hype address
        if hypervisor_addresses:
            _and.append({"address": {"$in": hypervisor_addresses}})

        # add to query
        if _and:
            _match["$and"] = _and
            query.insert(0, {"$match": _match})
        # debug_query = f"{query}"
        return query

    @staticmethod
    def query_user_shares_operations(user_address: str, chain: Chain) -> list[dict]:
        """Return the user status as per shares and qtty currently staked in the hypervisor and all its operations, including transfers

        Args:
            user_address (str): user address

        Returns:
            list[dict]: query
        """
        return [
            {
                "$match": {
                    "$and": [
                        {
                            "$or": [
                                {"src": user_address},
                                {"dst": user_address},
                                {"sender": user_address},
                                {"to": user_address},
                            ]
                        },
                        {"src": {"$ne": "0x0000000000000000000000000000000000000000"}},
                        {"dst": {"$ne": "0x0000000000000000000000000000000000000000"}},
                    ]
                }
            },
            {"$sort": {"blockNumber": 1}},
            {
                "$lookup": {
                    "from": "static",
                    "let": {"op_address": "$address"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$address", "$$op_address"]}}},
                        {"$limit": 1},
                        {
                            "$project": {
                                "address": "$address",
                                "symbol": "$symbol",
                                "pool": {
                                    "address": "$pool.address",
                                    "token0": "$pool.token0.address",
                                    "token1": "$pool.token1.address",
                                    "dex": "$pool.dex",
                                },
                                "dex": "$dex",
                            }
                        },
                        {"$unset": ["_id"]},
                    ],
                    "as": "static",
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "block": "$blockNumber",
                    "timestamp": "$timestamp",
                    "hypervisor": "$address",
                    "hypervisor_symbol": {"$arrayElemAt": ["$static.symbol", 0]},
                    "token0_address": {"$arrayElemAt": ["$static.pool.token0", 0]},
                    "token1_address": {"$arrayElemAt": ["$static.pool.token1", 0]},
                    "decimals_contract": "$decimals_contract",
                    "decimals_token0": "$decimals_token0",
                    "decimals_token1": "$decimals_token1",
                    "topic": "$topic",
                    "shares": {
                        "$ifNull": [
                            {
                                "$cond": [
                                    {
                                        "$or": [
                                            {"$eq": ["$topic", "deposit"]},
                                            {"$eq": ["$dst", user_address]},
                                        ]
                                    },
                                    {"$toDecimal": {"$ifNull": ["$qtty", "$shares"]}},
                                    {
                                        "$multiply": [
                                            {
                                                "$toDecimal": {
                                                    "$ifNull": ["$qtty", "$shares"]
                                                }
                                            },
                                            -1,
                                        ]
                                    },
                                ]
                            },
                            0,
                        ]
                    },
                    "token0": {
                        "$ifNull": [
                            {
                                "$cond": [
                                    {"$eq": ["$topic", "deposit"]},
                                    {"$toDecimal": "$qtty_token0"},
                                    {"$multiply": [{"$toDecimal": "$qtty_token0"}, -1]},
                                ]
                            },
                            0,
                        ]
                    },
                    "token1": {
                        "$ifNull": [
                            {
                                "$cond": [
                                    {"$eq": ["$topic", "deposit"]},
                                    {"$toDecimal": "$qtty_token1"},
                                    {"$multiply": [{"$toDecimal": "$qtty_token1"}, -1]},
                                ]
                            },
                            0,
                        ]
                    },
                }
            },
            {
                "$group": {
                    "_id": {"hype": "$hypervisor"},
                    "last_block": {"$last": "$block"},
                    "last_timestamp": {"$last": "$timestamp"},
                    "info": {
                        "$first": {
                            "hypervisor_symbol": "$hypervisor_symbol",
                            "token0_address": "$token0_address",
                            "token1_address": "$token1_address",
                            "decimals_hype": "$decimals_contract",
                            "decimals_token0": "$decimals_token0",
                            "decimals_token1": "$decimals_token1",
                        }
                    },
                    "last_shares": {"$sum": "$shares"},
                    "last_token0": {"$sum": "$token0"},
                    "last_token1": {"$sum": "$token1"},
                    "operations": {
                        "$push": {
                            "block": "$block",
                            "timestamp": "$timestamp",
                            "topic": "$topic",
                            "shares": "$shares",
                            "token0": "$token0",
                            "token1": "$token1",
                        }
                    },
                    "price_id_token0": {
                        "$push": {
                            "$concat": [
                                chain.database_name,
                                "_",
                                {"$toString": "$block"},
                                "_",
                                "$token0_address",
                            ]
                        }
                    },
                    "price_id_token1": {
                        "$push": {
                            "$concat": [
                                chain.database_name,
                                "_",
                                {"$toString": "$block"},
                                "_",
                                "$token1_address",
                            ]
                        }
                    },
                }
            },
            {"$addFields": {"hypervisor": "$_id.hype"}},
            {"$unset": "_id"},
        ]

    @staticmethod
    def query_rewarders_by_rewardRegistry() -> list[dict]:
        """return the query to build
               list of {
                    "_id": <rewarder_registry>,
                    "rewarders": [<rewarder_address>,...]
               }

        Returns:
            query:
        """
        return [
            {
                "$group": {
                    "_id": "$rewarder_registry",
                    "rewarders": {"$push": "$rewarder_address"},
                }
            }
        ]

    @staticmethod
    def query_all_user_Rewarder_transactions(
        user_address: str, rewarder_address: str
    ) -> list[dict]:
        """It will return a list of operations with the following Extra fields:
                qtty_in: qtty staked in the rewarder
                qtty_out: qtty unstaked from the rewarder
                staked_in_rewarder: qtty_in - qtty_out

                u can sum all operations <staked_in_rewarder> field to get the total staked in the rewarder

        Args:
            user_address (str):
            rewarder_address (str):

        Returns:
            list[dict]:
        """
        return [
            {
                "$match": {
                    "topic": "transfer",
                    "$or": [
                        {"dst": user_address},
                        {"src": user_address},
                    ],
                }
            },
            {
                "$addFields": {
                    "qtty_in": {
                        "$cond": [
                            {"$eq": ["$dst", rewarder_address]},
                            {"$toDecimal": "$qtty"},
                            0,
                        ]
                    },
                    "qtty_out": {
                        "$cond": [
                            {"$eq": ["$src", rewarder_address]},
                            {"$toDecimal": "$qtty"},
                            0,
                        ]
                    },
                    "user_address": {
                        "$cond": [{"$eq": ["$dst", rewarder_address]}, "$src", "$dst"]
                    },
                }
            },
            {
                "$addFields": {
                    "staked_in_rewarder": {"$subtract": ["$qtty_in", "$qtty_out"]},
                }
            },
        ]

    @staticmethod
    def query_user_allRewarder_transactions(
        user_address: str, rewarders_addresses: list[str]
    ) -> list[dict]:
        """Get all user transactions and summary status for all rewarders in the network with staked value gt 0
                returns a list of operations with the following fields:
                    "_id": <rewarder_address>,
                    "staked: <total_staked_in_rewarder>,
                    "operations": [<operation>,...]
                    "rewarder_data": < static rewarder information >
                    "hypervisor_data": < static hypervisor information >
        Args:
            user_address (str):
            rewarders_addresses (list[str]):

        Returns:
            list[dict]: _description_
        """
        return [
            {
                "$match": {
                    "topic": "transfer",
                    "src": {"$ne": "0x0000000000000000000000000000000000000000"},
                    "dst": {"$ne": "0x0000000000000000000000000000000000000000"},
                    "$or": [
                        {
                            "$and": [
                                {"dst": user_address},
                                {"src": {"$in": rewarders_addresses}},
                            ]
                        },
                        {
                            "$and": [
                                {"src": user_address},
                                {"dst": {"$in": rewarders_addresses}},
                            ]
                        },
                    ],
                }
            },
            {
                "$addFields": {
                    "qtty_in": {
                        "$cond": [
                            {"$in": ["$dst", rewarders_addresses]},
                            {"$toDecimal": "$qtty"},
                            0,
                        ]
                    },
                    "qtty_out": {
                        "$cond": [
                            {"$in": ["$src", rewarders_addresses]},
                            {"$toDecimal": "$qtty"},
                            0,
                        ]
                    },
                    "user_address": {
                        "$cond": [{"$eq": ["$dst", user_address]}, "$dst", "$src"]
                    },
                    "rewarder_address": {
                        "$cond": [{"$eq": ["$src", user_address]}, "$dst", "$src"]
                    },
                }
            },
            {
                "$addFields": {
                    "staked_in_rewarder": {"$subtract": ["$qtty_in", "$qtty_out"]},
                }
            },
            {
                "$group": {
                    "_id": {"$concat": ["$address", "_", "$rewarder_address"]},
                    "hypervisor_address": {"$first": "$address"},
                    "rewarder_address": {"$first": "$rewarder_address"},
                    "staked": {"$sum": "$staked_in_rewarder"},
                    "operations": {"$push": "$$ROOT"},
                }
            },
            {
                "$match": {
                    "staked": {"$gt": 0},
                }
            },
            {
                "$lookup": {
                    "from": "rewards_static",
                    "localField": "_id",
                    "foreignField": "id",
                    "as": "rewarder_data",
                }
            },
            {"$unwind": "$rewarder_data"},
            {
                "$lookup": {
                    "from": "static",
                    "localField": "hypervisor_address",
                    "foreignField": "address",
                    "as": "hypervisor_data",
                }
            },
            {"$unwind": "$hypervisor_data"},
        ]

    @staticmethod
    def query_latest_multifeedistributor(
        mfd_addresses: list[str] | None = None,
        hypervisor_addresses: list[str] | None = None,
        dex: str | None = None,
    ) -> list[dict]:
        match = {}
        if mfd_addresses:
            match["address"] = {"$in": mfd_addresses}
        if hypervisor_addresses:
            match["hypervisor_address"] = {"$in": hypervisor_addresses}
        if dex:
            match["dex"] = dex

        query = [
            {"$sort": {"address": -1}},
            {
                "$lookup": {
                    "from": "static",
                    "localField": "hypervisor_address",
                    "foreignField": "address",
                    "as": "hypervisor_static",
                }
            },
            {"$unwind": "$hypervisor_static"},
            {
                "$unset": [
                    "_id",
                    "id",
                    "last_updated_data.id",
                    "hypervisor_status._id",
                    "hypervisor_status.id",
                ]
            },
        ]
        if match:
            query.insert(0, {"$match": match})

        return query


class database_perps(db_collections_common):
    """Perps database class"""

    def __init__(
        self, mongo_url: str, db_name: str, db_collections: dict | None = None
    ):
        if db_collections is None:
            db_collections = {
                "ohlcv": {
                    "mono_indexes": {
                        "id": True,
                    },
                    "multi_indexes": [
                        [
                            ("timestamp", ASCENDING),
                            ("token", ASCENDING),
                            ("timeframe", ASCENDING),
                        ],
                    ],
                },
                "backtests": {
                    "mono_indexes": {
                        "id": True,
                    },
                    "multi_indexes": [
                        [
                            ("timestamp", ASCENDING),
                            ("strategy", ASCENDING),
                            ("token", ASCENDING),
                            ("timeframe", ASCENDING),
                            ("lookback", ASCENDING),
                            ("leverage", ASCENDING),
                        ],
                    ],
                },
            }

        else:
            logging.getLogger(__name__).warning(
                f" using custom db_collections on local dbatabase class for {db_name}:  {db_collections} "
            )

        super().__init__(
            mongo_url=mongo_url, db_name=db_name, db_collections=db_collections
        )

    async def get_backtests(
        self,
        token: str | None = None,
        timeframe: str | None = None,
        strategy: str | None = None,
        lookback: int | None = None,
        leverage: str | None = None,
        start_datetime: datetime | None = None,
        end_datetime: datetime | None = None,
    ):
        """Get backtests"""
        #
        find = {}
        if token:
            find["token"] = token
        if timeframe:
            find["timeframe"] = timeframe
        if strategy:
            find["strategy"] = strategy
        if lookback:
            find["lookback"] = lookback
        if leverage:
            find["leverage"] = leverage
        if start_datetime and end_datetime:
            find["timestamp"] = {"$gte": start_datetime, "$lte": end_datetime}
        elif start_datetime:
            find["timestamp"] = {"$gte": start_datetime}
        elif end_datetime:
            find["timestamp"] = {"$lte": end_datetime}

        return await self.get_items_from_database(
            collection_name="backtests", find=find
        )


### special databae for xtrade only
class database_xtrade(db_collections_common):

    def __init__(
        self, mongo_url: str, db_name: str, db_collections: dict | None = None
    ):
        if db_collections is None:
            db_collections = {
                "reward_operations": {
                    "mono_indexes": {
                        "id": True,
                        "blockNumber": False,
                        "address": False,
                        "user_address": False,
                        "timestamp": False,
                        "topic": False,
                        "reward_type": False,
                    },
                    "multi_indexes": [
                        [("blockNumber", ASCENDING), ("logIndex", ASCENDING)],
                    ],
                },
                "token_operations": {
                    "mono_indexes": {
                        "id": True,
                        "blockNumber": False,
                        "to": False,
                        "from": False,
                        "to_type": False,
                        "from_type": False,
                        "timestamp": False,
                    },
                    "multi_indexes": [
                        [("blockNumber", ASCENDING), ("logIndex", ASCENDING)],
                    ],
                },
                "user_rewards": {
                    "mono_indexes": {
                        "id": True,
                        "block": False,
                        "timestamp": False,
                        "user_address": False,
                        "hypervisor_address": False,
                        "rewarder_address": False,
                    },
                    "multi_indexes": [],
                },
                "leaderboard": {
                    "mono_indexes": {
                        "id": True,
                        "block": False,
                        "user_address": False,
                        "timestamp": False,
                        "tokenAddress": False,
                    },
                    "multi_indexes": [
                        [
                            ("user_address", ASCENDING),
                            ("timestamp", ASCENDING),
                            ("tokenAddress", ASCENDING),
                        ],
                    ],
                },
            }

        else:
            logging.getLogger(__name__).warning(
                f" using custom db_collections on xtrade dbatabase class for {db_name}:  {db_collections} "
            )

        super().__init__(
            mongo_url=mongo_url, db_name=db_name, db_collections=db_collections
        )
