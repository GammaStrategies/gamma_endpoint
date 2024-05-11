import asyncio
import time
from decimal import Decimal
import itertools
import logging
from math import prod
from sources.common.general.enums import Chain, Protocol
from sources.common.database.collection_endpoint import database_local

# TODO: restruct global config and local config
from sources.mongo.bins.helpers import global_database_helper, local_database_helper
from sources.subgraph.bins.config import MONGO_DB_URL


class user_analytics:
    def __init__(self, user_address: str, chain: Chain):
        self.user_address = user_address
        self.chain = chain
        self.operations = []
        self.result = {
            "user_address": user_address,
            "total_current_pnl_usd": 0,
            "total_deposit_value_usd": 0,
            "total_withdraw_value_usd": 0,
            "hypervisors": {},
        }

    def last_item(self, hypervisor_address: str) -> dict:
        """return the last user operation item from an specific hypervisor

        Args:
            hypervisor_address (str):

        Returns:
            dict: user operation item
        """
        if result := self.result["hypervisors"].get(hypervisor_address, []):
            return result[-1]
        return {}
        # return (
        #     self.result["hypervisors"].get(hypervisor_address, [])[-1]
        #     if hypervisor_address in self.result["hypervisors"] and len(
        #         self.result["hypervisors"][hypervisor_address]
        #     )
        #     else {}
        # )

    def create_new_operation_item(self, operation: dict) -> dict:
        operation_item = {
            "operation": operation["topic"],
            "block": operation["block"],
            "timestamp": operation["timestamp"],
            "period_seconds": operation["timestamp"]
            - self.last_item(operation["hypervisor_address"]).get("timestamp", 0),
            "pnl_usd": 0,
            "pnl_token0": 0,
            "pnl_token1": 0,
            "shares": operation["shares_in"]
            - operation["shares_out"]
            + self.last_item(operation["hypervisor_address"]).get("shares", 0),
            "shares_value_usd": 0,
            "underlying_token0_qtty": 0,
            "underlying_token1_qtty": 0,
            "fees_token0_qtty": 0,
            "fees_token1_qtty": 0,
            "fees_value_usd": 0,
            "fees_period_yield": 0,
            "fees_apr": 0,
            "fees_apy": 0,
            "control": {
                "total_deposit_token0_qtty": self.last_item(
                    operation["hypervisor_address"]
                )
                .get("control", {})
                .get("total_deposit_token0_qtty", 0),
                "total_deposit_token1_qtty": self.last_item(
                    operation["hypervisor_address"]
                )
                .get("control", {})
                .get("total_deposit_token1_qtty", 0),
                "total_deposit_value_usd": self.last_item(
                    operation["hypervisor_address"]
                )
                .get("control", {})
                .get("total_deposit_value_usd", 0),
                "total_withdraw_token0_qtty": self.last_item(
                    operation["hypervisor_address"]
                )
                .get("control", {})
                .get("total_withdraw_token0_qtty", 0),
                "total_withdraw_token1_qtty": self.last_item(
                    operation["hypervisor_address"]
                )
                .get("control", {})
                .get("total_withdraw_token1_qtty", 0),
                "total_withdraw_value_usd": self.last_item(
                    operation["hypervisor_address"]
                )
                .get("control", {})
                .get("total_withdraw_value_usd", 0),
            },
        }
        # underlying token ----------------------------
        if "underlying_token0_per_share" in operation:
            logging.getLogger(__name__).debug(
                f" no underlying_token0_per_share in operation -> {operation}"
            )
        operation_item["underlying_token0_qtty"] = (
            operation_item["shares"] * operation["underlying_token0_per_share"]
        )
        operation_item["underlying_token1_qtty"] = (
            operation_item["shares"] * operation["underlying_token1_per_share"]
        )
        # shares value --------------------------------
        operation_item["shares_value_usd"] = (
            operation_item["underlying_token0_qtty"] * operation["price_usd_token0"]
            + operation_item["underlying_token1_qtty"] * operation["price_usd_token1"]
        )
        # check operation topic

        # fees ----------------------------------------
        operation_item["fees_token0_qtty"] = operation[
            "fees_token0_in"
        ] + self.last_item(operation["hypervisor_address"]).get("fees_token0_qtty", 0)
        operation_item["fees_token1_qtty"] = operation[
            "fees_token1_in"
        ] + self.last_item(operation["hypervisor_address"]).get("fees_token1_qtty", 0)
        operation_item["fees_value_usd"] = (
            operation_item["fees_token0_qtty"] * operation["price_usd_token0"]
            + operation_item["fees_token1_qtty"] * operation["price_usd_token1"]
        )
        #
        #     fee yield
        #
        period_fees_usd = (
            operation["fees_token0_in"] * operation["price_usd_token0"]
            + operation["fees_token1_in"] * operation["price_usd_token1"]
        )
        operation_item["fees_period_yield"] = (
            (period_fees_usd / operation_item["period_seconds"])
            if operation_item["period_seconds"]
            else 0
        )
        #    fee apr
        # apr n apy
        cum_fee_return = prod(
            [
                1 + x["fees_period_yield"]
                for x in self.result["hypervisors"].get(
                    operation["hypervisor_address"], []
                )
            ]
        )
        total_period_seconds = sum(
            [
                Decimal(x["period_seconds"])
                for x in self.result["hypervisors"].get(
                    operation["hypervisor_address"], []
                )
            ]
        )
        day_in_seconds = Decimal("86400")
        year_in_seconds = Decimal("365") * day_in_seconds
        operation_item["fees_apr"] = (
            ((cum_fee_return - 1) * (year_in_seconds / total_period_seconds))
            if total_period_seconds
            else 0
        )
        operation_item["fees_apy"] = (
            (
                (1 + (cum_fee_return - 1) * (day_in_seconds / total_period_seconds))
                ** 365
                - 1
            )
            if total_period_seconds
            else 0
        )

        # modify fields by type
        if operation["topic"] == "deposit":
            self.process_deposit(operation_item, operation)
        elif operation["topic"] == "withdraw":
            self.process_withdraw(operation_item, operation)
        elif operation["topic"] == "transfer":
            self.process_transfer(operation_item, operation)
        elif operation["topic"] in ["rebalance", "zeroBurn"]:
            self.process_fee(operation_item, operation)
        else:
            raise Exception(f"unknown operation topic -> {operation['topic']} ")

        # pnl -----------------------------------------
        # (-totaldeposits + totalwithdraws) + (current sharesvalue)
        operation_item["pnl_usd"] = (
            operation_item.get("control", {})["total_withdraw_value_usd"]
            - operation_item.get("control", {})["total_deposit_value_usd"]
            + operation_item["shares_value_usd"]
        )
        operation_item["pnl_token0"] = (
            operation_item.get("control", {})["total_withdraw_token0_qtty"]
            - operation_item.get("control", {})["total_deposit_token0_qtty"]
            + operation_item["underlying_token0_qtty"]
        )
        operation_item["pnl_token1"] = (
            operation_item.get("control", {})["total_withdraw_token1_qtty"]
            - operation_item.get("control", {})["total_deposit_token1_qtty"]
            + operation_item["underlying_token1_qtty"]
        )

        return operation_item

    def process_operations(self, operations: list[dict]) -> dict:
        """create a status summary of user operations by hypervisor

        Args:
            operations (list[dict]):

        Returns:
            dict:
        """
        for operation in operations:
            # create hype in result if not exists
            if operation["hypervisor_address"] not in self.result["hypervisors"]:
                self.result["hypervisors"][operation["hypervisor_address"]] = []

            # add operation to result
            self.result["hypervisors"][operation["hypervisor_address"]].append(
                self.create_new_operation_item(operation)
            )

        # add globals
        for operations in self.result["hypervisors"].values():
            if operations:
                self.result["total_current_pnl_usd"] += operations[-1]["pnl_usd"]
                self.result["total_deposit_value_usd"] += operations[-1]["control"][
                    "total_deposit_value_usd"
                ]
                self.result["total_withdraw_value_usd"] += operations[-1]["control"][
                    "total_withdraw_value_usd"
                ]

        return self.result

    def process_withdraw(self, operation_item: dict, operation: dict):
        operation_item.get("control", {})["total_withdraw_token0_qtty"] += operation[
            "token0_out"
        ]
        operation_item.get("control", {})["total_withdraw_token1_qtty"] += operation[
            "token1_out"
        ]
        operation_item.get("control", {})["total_withdraw_value_usd"] += (
            operation["token0_out"] * operation["price_usd_token0"]
            + operation["token1_out"] * operation["price_usd_token1"]
        )

    def process_deposit(self, operation_item: dict, operation: dict):
        operation_item.get("control", {})["total_deposit_token0_qtty"] += operation[
            "token0_in"
        ]
        operation_item.get("control", {})["total_deposit_token1_qtty"] += operation[
            "token1_in"
        ]
        operation_item.get("control", {})["total_deposit_value_usd"] += (
            operation["token0_in"] * operation["price_usd_token0"]
            + operation["token1_in"] * operation["price_usd_token1"]
        )

    def process_transfer(self, operation_item: dict, operation: dict):
        self.process_deposit(operation_item, operation)
        self.process_withdraw(operation_item, operation)

    def process_fee(self, operation_item: dict, operation: dict):
        pass


async def get_user_analytic_data(
    chain: Chain,
    address: str,
    block_ini: int = 0,
    block_end: int = 0,
):
    # build query
    find = {"user_address": address}
    if block_ini:
        find["block"] = {"$gte": block_ini}
    if block_end:
        find["block"] = {"$lte": block_end}

    # get operations
    return user_analytics(user_address=address, chain=chain).process_operations(
        operations=[
            database_local.convert_d128_to_decimal(operation)
            for operation in await database_local(
                mongo_url=MONGO_DB_URL, db_name=f"{chain.database_name}_gamma"
            ).get_items_from_database(
                collection_name="user_operations",
                find=find,
                sort=[("block", 1)],
            )
        ]
    )


async def get_user_historic_info(
    chain: Chain, address: str, timestamp_ini: int = 0, timestamp_end: int = 0
):
    db_name = f"{chain.database_name}_gamma"
    local_db_helper = database_local(mongo_url=MONGO_DB_URL, db_name=db_name)

    return await local_db_helper.get_user_operations_status(
        user_address=address, timestamp_ini=timestamp_ini, timestamp_end=timestamp_end
    )


# user operations collection
async def get_user_operations(
    chain: Chain,
    protocol: Protocol | None = None,
    user_address: str | None = None,
    hypervisor_address_list: list[str] | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    net_position_usd_threshold: float | None = None,
    deposits_usd_threshold: float | None = None,
) -> list[dict]:

    # TODO: filter net_position_usd_threshold and deposits_usd_threshold

    try:
        # create queries to execute

        if protocol and not hypervisor_address_list:
            # get hype addresses for this particular protocol
            hypervisor_address_list = [
                x["address"]
                for x in await local_database_helper(
                    network=chain
                ).get_items_from_database(
                    collection_name="static",
                    find={"dex": protocol.database_name},
                )
            ]

        return [
            global_database_helper().convert_d128_to_decimal(x)
            for x in await local_database_helper(network=chain).get_items_from_database(
                collection_name="user_operations",
                aggregate=query_user_operations(
                    user_address=user_address,
                    hypervisor_address_list=hypervisor_address_list,
                    block_ini=block_ini,
                    block_end=block_end,
                    timestamp_ini=timestamp_ini,
                    timestamp_end=timestamp_end,
                ),
            )
        ]
    except Exception as e:
        logging.getLogger(__name__).exception(f"Error in get_user_operations: {e}")
        return [{"error": " An error occurred while processing the data"}]


# Queries:
def query_user_operations(
    user_address: str | None = None,
    hypervisor_address_list: list[str] | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
) -> list[dict]:
    """Return a query to get user operations and balances

    Args:
        chain (Chain): _description_
        user_address (str | None, optional): _description_. Defaults to None.
        hypervisor_address_list (list[str] | None, optional): _description_. Defaults to None.
        block_ini (int | None, optional): _description_. Defaults to None.
        block_end (int | None, optional): _description_. Defaults to None.
        timestamp_ini (int | None, optional): _description_. Defaults to None.
        timestamp_end (int | None, optional): _description_. Defaults to None.

    Returns:
        list[dict]: {
            "user_address": str,
            "hypervisor_address": str,
            "shares_balance_ini": str,
            "shares_balance_end": str,
            "operations": list[dict]
                    {
                        "block": int,
                        "timestamp": int,
                        "shares": dict,
                        "tokens_flow": dict,
                        "prices": dict,
                        "topic": str,
                        "transactionHash": str,
                        "shadowed_user_address": str
                }
    """

    _match = {}

    if block_ini:
        _match["block"] = {"$lt": block_ini}
    if timestamp_ini:
        _match["timestamp"] = {"$lt": timestamp_ini}
    if user_address:
        _match["user_address"] = user_address
    if hypervisor_address_list:
        _match["hypervisor_address"] = {"$in": hypervisor_address_list}

    # build query
    _query = [
        {"$sort": {"block": 1}},
        {
            "$group": {
                "_id": {"user": "$user_address", "hype": "$hypervisor_address"},
                "starting_block": {"$last": "$block"},
            }
        },
        {
            "$lookup": {
                "from": "user_operations",
                "let": {
                    "op_hype": "$_id.hype",
                    "op_user": "$_id.user",
                    "op_block": "$starting_block",
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$hypervisor_address", "$$op_hype"]},
                                    {"$eq": ["$user_address", "$$op_user"]},
                                    {"$gte": ["$block", "$$op_block"]},
                                    {"$lte": ["$block", 205967681]},
                                ]
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "isReal": 0,
                            "logIndex": 0,
                            "customIndex": 0,
                            "id": 0,
                        }
                    },
                    {
                        "$lookup": {
                            "from": "status",
                            "let": {
                                "op_hype": "$hypervisor_address",
                                "op_block": "$block",
                            },
                            "pipeline": [
                                {
                                    "$match": {
                                        "$expr": {
                                            "$and": [
                                                {"$eq": ["$address", "$$op_hype"]},
                                                {"$eq": ["$block", "$$op_block"]},
                                            ]
                                        }
                                    }
                                },
                                {
                                    "$project": {
                                        "_id": 0,
                                        "hypervisor_decimals": "$decimals",
                                        "token0_decimals": "$pool.token0.decimals",
                                        "token1_decimals": "$pool.token1.decimals",
                                        "totalSupply": {"$toDecimal": "$totalSupply"},
                                        "underlying_value": {
                                            "token0": {
                                                "$sum": [
                                                    {
                                                        "$toDecimal": "$totalAmounts.total0"
                                                    },
                                                    {
                                                        "$toDecimal": "$fees_uncollected.lps_qtty_token0"
                                                    },
                                                ]
                                            },
                                            "token1": {
                                                "$sum": [
                                                    {
                                                        "$toDecimal": "$totalAmounts.total1"
                                                    },
                                                    {
                                                        "$toDecimal": "$fees_uncollected.lps_qtty_token1"
                                                    },
                                                ]
                                            },
                                        },
                                    }
                                },
                            ],
                            "as": "hypervisor_status",
                        }
                    },
                    {
                        "$addFields": {
                            "hypervisor_status": {"$first": "$hypervisor_status"},
                            "shares": {
                                "flow": {"$toDecimal": "$shares.flow"},
                                "balance": {"$toDecimal": "$shares.balance"},
                            },
                            "tokens_flow": {
                                "token0": {"$toDecimal": "$tokens_flow.token0"},
                                "token1": {"$toDecimal": "$tokens_flow.token1"},
                            },
                        }
                    },
                    {
                        "$addFields": {
                            "shares.balance_percentage": {
                                "$divide": [
                                    "$shares.balance",
                                    "$hypervisor_status.totalSupply",
                                ]
                            },
                            "shares.balance_token0": {
                                "$cond": [
                                    {"$gt": ["$shares.balance", 0]},
                                    {
                                        "$multiply": [
                                            {
                                                "$divide": [
                                                    "$shares.balance",
                                                    "$hypervisor_status.totalSupply",
                                                ]
                                            },
                                            "$hypervisor_status.underlying_value.token0",
                                        ]
                                    },
                                    0,
                                ]
                            },
                            "shares.balance_token1": {
                                "$cond": [
                                    {"$gt": ["$shares.balance", 0]},
                                    {
                                        "$multiply": [
                                            {
                                                "$divide": [
                                                    "$shares.balance",
                                                    "$hypervisor_status.totalSupply",
                                                ]
                                            },
                                            "$hypervisor_status.underlying_value.token1",
                                        ]
                                    },
                                    0,
                                ]
                            },
                            "hypervisor_status.underlying_value.usd": {
                                "$sum": [
                                    {
                                        "$multiply": [
                                            "$prices.token0",
                                            {
                                                "$divide": [
                                                    "$hypervisor_status.underlying_value.token0",
                                                    {
                                                        "$pow": [
                                                            10,
                                                            "$hypervisor_status.token0_decimals",
                                                        ]
                                                    },
                                                ]
                                            },
                                        ]
                                    },
                                    {
                                        "$multiply": [
                                            "$prices.token1",
                                            {
                                                "$divide": [
                                                    "$hypervisor_status.underlying_value.token1",
                                                    {
                                                        "$pow": [
                                                            10,
                                                            "$hypervisor_status.token1_decimals",
                                                        ]
                                                    },
                                                ]
                                            },
                                        ]
                                    },
                                ]
                            },
                            "tokens_flow.usd": {
                                "$sum": [
                                    {
                                        "$multiply": [
                                            "$prices.token0",
                                            {
                                                "$divide": [
                                                    "$tokens_flow.token0",
                                                    {
                                                        "$pow": [
                                                            10,
                                                            "$hypervisor_status.token0_decimals",
                                                        ]
                                                    },
                                                ]
                                            },
                                        ]
                                    },
                                    {
                                        "$multiply": [
                                            "$prices.token1",
                                            {
                                                "$divide": [
                                                    "$tokens_flow.token1",
                                                    {
                                                        "$pow": [
                                                            10,
                                                            "$hypervisor_status.token1_decimals",
                                                        ]
                                                    },
                                                ]
                                            },
                                        ]
                                    },
                                ]
                            },
                            "prices.share": {
                                "$cond": [
                                    {
                                        "$and": [
                                            {"$gt": ["$tokens_flow.token0", 0]},
                                            {"$gt": ["$tokens_flow.token1", 0]},
                                        ]
                                    },
                                    {
                                        "$divide": [
                                            {
                                                "$divide": [
                                                    "$hypervisor_status.totalSupply",
                                                    {
                                                        "$pow": [
                                                            10,
                                                            "$hypervisor_status.hypervisor_decimals",
                                                        ]
                                                    },
                                                ]
                                            },
                                            {
                                                "$sum": [
                                                    {
                                                        "$multiply": [
                                                            "$prices.token0",
                                                            {
                                                                "$divide": [
                                                                    "$hypervisor_status.underlying_value.token0",
                                                                    {
                                                                        "$pow": [
                                                                            10,
                                                                            "$hypervisor_status.token0_decimals",
                                                                        ]
                                                                    },
                                                                ]
                                                            },
                                                        ]
                                                    },
                                                    {
                                                        "$multiply": [
                                                            "$prices.token1",
                                                            {
                                                                "$divide": [
                                                                    "$hypervisor_status.underlying_value.token1",
                                                                    {
                                                                        "$pow": [
                                                                            10,
                                                                            "$hypervisor_status.token1_decimals",
                                                                        ]
                                                                    },
                                                                ]
                                                            },
                                                        ]
                                                    },
                                                ]
                                            },
                                        ]
                                    },
                                    0,
                                ]
                            },
                        }
                    },
                    {"$sort": {"block": 1}},
                ],
                "as": "operations",
            }
        },
        {
            "$project": {
                "_id": 0,
                "user": "$_id.user",
                "hypervisor_address": "$_id.hype",
                "operations": "$operations",
            }
        },
    ]

    if _match:
        _query.insert(0, {"$match": _match})

    #  query
    return _query
