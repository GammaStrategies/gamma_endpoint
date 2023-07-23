from decimal import Decimal
import logging
from math import prod
from sources.common.general.enums import Chain
from sources.common.database.collection_endpoint import database_local

# TODO: restruct global config and local config
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


# async def get_user_analytic_data2(
#     chain: Chain,
#     address: str,
#     block_ini: int = 0,
#     block_end: int = 0,
# ):
#     db_name = f"{chain.database_name}_gamma"
#     local_db_helper = database_local(mongo_url=MONGO_DB_URL, db_name=db_name)
#     return await local_db_helper.get_user_status(
#         address=address, block_ini=block_ini, block_end=block_end
#     )


# async def get_user_analytic_data3(
#     chain: Chain,
#     address: str,
#     block_ini: int = 0,
#     block_end: int = 0,
# ):
#     result = {}

#     # build query
#     find = {"user_address": address}
#     if block_ini:
#         find["block"] = {"$gte": block_ini}
#     if block_end:
#         find["block"] = {"$lte": block_end}
#     # loop thru operations
#     for operation in await database_local(
#         mongo_url=MONGO_DB_URL, db_name=f"{chain.database_name}_gamma"
#     ).get_items_from_database(
#         collection_name="user_operations",
#         find=find,
#         sort=[("block", 1)],
#     ):
#         operation = database_local.convert_d128_to_decimal(operation)
#         if operation["hypervisor_address"] not in result:
#             result[operation["hypervisor_address"]] = []

#         # add to result
#         result[operation["hypervisor_address"]].append(
#             user_analytic_data_helper(
#                 result[operation["hypervisor_address"]], operation
#             )
#         )

#     return result


# def user_analytic_data_helper(hypervisor_result: list, operation: dict) -> dict:
#     # get last values
#     last_shares_qtty = Decimal("0")
#     last_operation_timestamp = operation["timestamp"]
#     last_fees_token0_qtty = Decimal("0")
#     last_fees_token1_qtty = Decimal("0")
#     last_shares_value_usd = Decimal("0")
#     last_pnl = Decimal("0")
#     last_pnl_token0 = Decimal("0")
#     last_pnl_token1 = Decimal("0")
#     if hypervisor_result:
#         last_shares_qtty = hypervisor_result[-1]["shares"]
#         last_operation_timestamp = hypervisor_result[-1]["timestamp"]
#         last_fees_token0_qtty = hypervisor_result[-1]["fees_token0_qtty"]
#         last_fees_token1_qtty = hypervisor_result[-1]["fees_token1_qtty"]
#         last_shares_value_usd = hypervisor_result[-1]["shares_value_usd"]
#         last_pnl = hypervisor_result[-1]["pnl_usd"]
#         last_pnl_token0 = hypervisor_result[-1]["pnl_token0"]
#         last_pnl_token1 = hypervisor_result[-1]["pnl_token1"]

#     # start building user operation result
#     user_operation_result = {
#         "operation": operation["topic"],
#         "block": operation["block"],
#         "timestamp": operation["timestamp"],
#         "period_seconds": operation["timestamp"] - last_operation_timestamp,
#         "pnl_usd": 0,
#         "pnl_token0": 0,
#         "pnl_token1": 0,
#         "shares": operation["shares_in"] - operation["shares_out"] + last_shares_qtty,
#         "shares_value_usd": 0,
#         "underlying_token0_qtty": 0,
#         "underlying_token1_qtty": 0,
#         "fees_token0_qtty": 0,
#         "fees_token1_qtty": 0,
#         "fees_value_usd": 0,
#         "fees_period_yield": 0,
#         "fees_apr": 0,
#         "fees_apy": 0,
#     }

#     user_operation_result["shares_value_usd"] = (
#         user_operation_result["shares"]
#         * operation["underlying_token0_per_share"]
#         * operation["price_usd_token0"]
#         + user_operation_result["shares"]
#         * operation["underlying_token1_per_share"]
#         * operation["price_usd_token1"]
#     )

#     user_operation_result["underlying_token0_qtty"] = (
#         user_operation_result["shares"] * operation["underlying_token0_per_share"]
#     )
#     user_operation_result["underlying_token1_qtty"] = (
#         user_operation_result["shares"] * operation["underlying_token1_per_share"]
#     )

#     user_operation_result["fees_token0_qtty"] = (
#         operation["fees_token0_in"] + last_fees_token0_qtty
#     )
#     user_operation_result["fees_token1_qtty"] = (
#         operation["fees_token1_in"] + last_fees_token1_qtty
#     )
#     user_operation_result["fees_value_usd"] = (
#         user_operation_result["fees_token0_qtty"] * operation["price_usd_token0"]
#         + user_operation_result["fees_token1_qtty"] * operation["price_usd_token1"]
#     )

#     period_fees_usd = (
#         operation["fees_token0_in"] * operation["price_usd_token0"]
#         + operation["fees_token1_in"] * operation["price_usd_token1"]
#     )
#     user_operation_result["fees_period_yield"] = (
#         (period_fees_usd / user_operation_result["period_seconds"])
#         if user_operation_result["period_seconds"]
#         else 0
#     )

#     # apr n apy
#     cum_fee_return = prod([1 + x["fees_period_yield"] for x in hypervisor_result])
#     total_period_seconds = sum(
#         [Decimal(x["period_seconds"]) for x in hypervisor_result]
#     )
#     day_in_seconds = Decimal("86400")
#     year_in_seconds = Decimal("365") * day_in_seconds
#     user_operation_result["fees_apr"] = (
#         ((cum_fee_return - 1) * (year_in_seconds / total_period_seconds))
#         if total_period_seconds
#         else 0
#     )
#     user_operation_result["fees_apy"] = (
#         (
#             (1 + (cum_fee_return - 1) * (day_in_seconds / total_period_seconds)) ** 365
#             - 1
#         )
#         if total_period_seconds
#         else 0
#     )

#     # pnl = (current shares * current tokenX per share) - (current shares *  - last tokenX per share))
#     if last_shares_value_usd > 0:
#         user_operation_result["pnl_usd"] = (
#             last_pnl + user_operation_result["shares_value_usd"] - last_shares_value_usd
#         )

#         user_operation_result["pnl_token0"] = last_pnl_token0 + (
#             user_operation_result["underlying_token0_qtty"]
#             - hypervisor_result[-1]["underlying_token0_qtty"]
#         )

#         user_operation_result["pnl_token1"] = last_pnl_token1 + (
#             user_operation_result["underlying_token1_qtty"]
#             - hypervisor_result[-1]["underlying_token1_qtty"]
#         )
#         # withdraw
#         if user_operation_result["operation"] == "withdraw":
#             shares_to_withdraw = operation["shares_out"] - operation["shares_in"]
#             share_to_withdraw_percent = shares_to_withdraw / last_shares_qtty
#             # add withraw value to pnl
#             user_operation_result["pnl_usd"] += (
#                 operation["token0_out"] - operation["token0_in"]
#             ) * operation["price_usd_token0"] + (
#                 operation["token1_out"] - operation["token1_in"]
#             ) * operation[
#                 "price_usd_token1"
#             ]
#             user_operation_result["pnl_token0"] += (
#                 operation["token0_out"] - operation["token0_in"]
#             )
#             user_operation_result["pnl_token1"] += (
#                 operation["token1_out"] - operation["token1_in"]
#             )
#             # substract withdraw share % from fees

#     return user_operation_result


async def get_user_historic_info(
    chain: Chain, address: str, timestamp_ini: int = 0, timestamp_end: int = 0
):
    db_name = f"{chain.database_name}_gamma"
    local_db_helper = database_local(mongo_url=MONGO_DB_URL, db_name=db_name)

    return await local_db_helper.get_user_operations_status(
        user_address=address, timestamp_ini=timestamp_ini, timestamp_end=timestamp_end
    )
