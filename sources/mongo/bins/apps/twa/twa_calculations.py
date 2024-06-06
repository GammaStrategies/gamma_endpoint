import logging
from sources.common.general.enums import Chain, Protocol
from sources.mongo.bins.apps.twa.queries import query_hypervisor_operations_twa
from sources.mongo.bins.helpers import global_database_helper, local_database_helper


# user operations collection
async def get_hypervisor_operations_for_twa(
    chain: Chain,
    hypervisor_address: str,
    block_ini: int | None = None,
    block_end: int | None = None,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
) -> list[dict]:
    """Get all user operations for a given hypervisor address in a given range of blocks or timestamps
        Sorted by block number

    Args:
        chain (Chain):
        hypervisor_address (str):
        block_ini (int | None, optional): . Defaults to None.
        block_end (int | None, optional): . Defaults to None.
        timestamp_ini (int | None, optional): . Defaults to None.
        timestamp_end (int | None, optional): . Defaults to None.

    Returns:
        list[dict]:
    """

    try:
        # create queries to execute
        return [
            global_database_helper().convert_d128_to_decimal(x)
            for x in await local_database_helper(network=chain).get_items_from_database(
                collection_name="user_operations",
                aggregate=query_hypervisor_operations_twa(
                    hypervisor_address=hypervisor_address,
                    block_ini=block_ini,
                    block_end=block_end,
                    timestamp_ini=timestamp_ini,
                    timestamp_end=timestamp_end,
                ),
            )
        ]
    except Exception as e:
        logging.getLogger(__name__).exception(f"Error in get_user_operations: {e}")

    return None


async def gamma_rewards_TWA_calculation_test(
    chain: Chain,
    hypervisor_address: str,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
):

    # decide whether to use timestamp or block
    timevar_txt = "timestamp"
    timevar_ini = timestamp_ini
    timevar_end = timestamp_end
    if block_ini:
        timevar_txt = "block"
        timevar_ini = block_ini
        timevar_end = block_end

    # get all hype operations
    hypervisor_operations = await get_hypervisor_operations_for_twa(
        chain=chain,
        hypervisor_address=hypervisor_address,
        block_ini=block_ini,
        block_end=block_end,
        timestamp_ini=timestamp_ini,
        timestamp_end=timestamp_end,
    )
    if not hypervisor_operations:
        return f"No hypervisor operations found for {hypervisor_address}"

    # result_data helper variable {
    #             <hype_address>: {
    #                   "users": { <user>: { "initial_balance": <initial_balance>, "final_balance": <final_balance>, "operations":[]}}
    #                   f"{timevar_txt}s":{ <block/timestamp>: <hypervisor_status> } }
    #                       }
    result_data = {
        "hypervisor_address": hypervisor_address,
        f"{timevar_txt}_ini": timevar_ini or hypervisor_operations[0][timevar_txt],
        f"{timevar_txt}_end": timevar_end or hypervisor_operations[-1][timevar_txt],
        "totalSupply_ini": None,
        "totalSupply_end": None,
        "users": {},
        "total_twa": 0,
    }
    # create a global denominator to calc twa for each user
    global_twa_denominator = (
        result_data[f"{timevar_txt}_end"] - result_data[f"{timevar_txt}_ini"]
    )
    last_timevar = timevar_ini
    for operation in hypervisor_operations:

        # easy access vars
        current_total_supply = operation["hypervisor_status"]["totalSupply"]
        current_timevar = operation[timevar_txt]

        # for all users already processed

        # check if user is in the dict
        if operation["user_address"] not in result_data["users"]:
            result_data["users"][operation["user_address"]] = {
                "twa": None,
                "aggregated_numerator": 0,
                "initial_balance": 0,
                "final_balance": 0,
                "current_balance": 0,
                "operations": [],
            }

        # if block is lower than initial block, this is the initial balance for the user
        if current_timevar < timevar_ini:
            result_data["users"][operation["user_address"]]["initial_balance"] = (
                operation["shares"]["balance"]
            )
            # temporary final balance is the same as initial balance
            result_data["users"][operation["user_address"]]["final_balance"] = (
                operation["shares"]["balance"]
            )
            # set current balance ( most recent balance)
            result_data["users"][operation["user_address"]]["current_balance"] = (
                operation["shares"]["balance"]
            )
            # set initial total supply
            result_data["totalSupply_ini"] = current_total_supply
        else:
            # this is an operation after the initial block

            # () calculate all other users TWA using the current operation's total supply as new total supply, mantaining each user balances
            for user_address, user_data in result_data["users"].items():
                if user_address == operation["user_address"]:
                    # do not calculate TWA for the user in the current operation
                    continue
                # calculate TWA
                user_data["aggregated_numerator"] += calculate_TWpercentage(
                    user_data["current_balance"],
                    current_total_supply,
                    current_timevar,
                    last_timevar,
                )
                # result_data["users"][user_address]["operations"].append(
                #     {
                #         "block": operation["block"],
                #         "timestamp": operation["timestamp"],
                #         "shares": {
                #             "balance": user_data["current_balance"],
                #         },
                #         "hypervisor_status": {
                #             "totalSupply": current_total_supply,
                #         },
                #     }
                # )

                # calculate the user's final twa ( will do this every time a new operation is processed)
                result_data["total_twa"] += user_data["aggregated_numerator"]

                # calculate the user's final twa ( will do this every time a new operation is processed)
                user_data["twa"] = (
                    user_data["aggregated_numerator"] / global_twa_denominator
                )

            # () calculate TWA for the user using the current operation's total supply as new total supply
            result_data["users"][operation["user_address"]][
                "aggregated_numerator"
            ] += calculate_TWpercentage(
                operation["shares"]["balance"],
                current_total_supply,
                current_timevar,
                last_timevar,
            )

            #
            result_data["total_twa"] += result_data["users"][operation["user_address"]][
                "aggregated_numerator"
            ]

            # calculate the user's final twa ( will do this every time a new operation is processed)
            result_data["users"][operation["user_address"]]["twa"] = (
                result_data["users"][operation["user_address"]]["aggregated_numerator"]
                / global_twa_denominator
            )

            # set user's final balance as current balance
            result_data["users"][operation["user_address"]]["final_balance"] = (
                operation["shares"]["balance"]
            )

            # set current balance ( most recent balance)
            result_data["users"][operation["user_address"]]["current_balance"] = (
                operation["shares"]["balance"]
            )

            # append operation to the user's operations list
            # result_data["users"][operation["user_address"]]["operations"].append(
            #     operation
            # )

            # set last timevar as current timevar
            last_timevar = current_timevar

            # set end total supply
            result_data["totalSupply_end"] = current_total_supply

    # calculate total twa by adding up all user twas
    result_data["total_twa2"] = sum(
        [user_data["twa"] for user_data in result_data["users"].values()]
    )

    result_data["total_twa"] = result_data["total_twa"] / global_twa_denominator

    return result_data

    # calculate TWA from hype_data created
    for hype_address, hype_data_item in hype_data.items():
        # get initial/end hypervisor supply ( filter blocks lower than initial block from f"{timevar_txt}s" and get the max one)
        initial_hypervisor_supply = max(
            [
                status["totalSupply"]
                for block, status in hype_data_item[f"{timevar_txt}s"].items()
                if block < timevar_ini
            ]
        )
        end_hypervisor_supply = max(
            [
                status["totalSupply"]
                for block, status in hype_data_item[f"{timevar_txt}s"].items()
                if block > timevar_ini
            ]
        )
        hype_data_item["initial_hypervisor_supply"] = initial_hypervisor_supply
        hype_data_item["end_hypervisor_supply"] = end_hypervisor_supply
        hype_data_item["total_twa"] = 0

        # delete_zero balances
        users_to_remove = []
        for user_address, user_data in hype_data_item["users"].items():

            last_time = timevar_ini
            user_twa_numerator = 0
            # denominator is time1 - time0
            # check if no block_end or timestamp_end, use last block or timestamp from f"{timevar_txt}s"
            timevar_end = (
                block_end
                or timestamp_end
                or max(list(hype_data_item[f"{timevar_txt}s"].keys()))
            )
            user_twa_denominator = timevar_end - timevar_ini
            # loop thu all operations
            for operation in user_data["operations"]:

                # this operation should be after the initial block/timestamp
                if operation[timevar_txt] < timevar_ini:
                    # error should never happen
                    continue

                # if this is the first operation, use the initial balance
                if last_time == timevar_ini:
                    user_twa_numerator += calculate_twa(
                        user_data["initial_balance"],
                        initial_hypervisor_supply,
                        operation[timevar_txt],
                        last_time,
                    )
                else:
                    user_twa_numerator += calculate_twa(
                        operation["shares"]["balance"] - operation["shares"]["flow"],
                        operation["hypervisor_status"]["totalSupply"],
                        operation[timevar_txt],
                        last_time,
                    )

                # change last time
                last_time = operation[timevar_txt]

            # calculate final step ( from last operation to end)
            # check if last operation is the same as the initial block/timestamp
            if last_time == timevar_ini:
                # if so, use initial balance
                user_twa_numerator += calculate_twa(
                    user_data["initial_balance"],
                    initial_hypervisor_supply,
                    timevar_end,
                    last_time,
                )
            else:
                # else, use final balance
                user_twa_numerator += calculate_twa(
                    user_data["final_balance"],
                    end_hypervisor_supply,
                    timevar_end,
                    last_time,
                )
            last_time = timevar_end

            # calculate TWA
            user_twa = user_twa_numerator / user_twa_denominator
            # add to user_data
            user_data["twa"] = user_twa
            # add to total_twa
            hype_data_item["total_twa"] += user_twa

            # check if user_twa is zero
            if user_twa == 0:
                users_to_remove.append(user_address)

        # remove users with zero TWA
        for user_address in users_to_remove:
            hype_data_item["users"].pop(user_address)
        # remove f"{timevar_txt}s" from hype_data_item
        hype_data_item.pop(f"{timevar_txt}s")

    # by adding all twas

    return hype_data


def calculate_TWpercentage(b, s, t1, t0):
    """Helper function to calculate the time weighted percentage of a balance

    Args:
        b :
        s :
        t1:
        t0:

    Returns:

    """
    return (b / s) * (t1 - t0)


##### SPREADSHEET CALCULATION #####


async def gamma_rewards_TWA_calculation(
    chain: Chain,
    hypervisor_address: str,
    timestamp_ini: int | None = None,
    timestamp_end: int | None = None,
    block_ini: int | None = None,
    block_end: int | None = None,
):

    # decide whether to use timestamp or block
    timevar_txt = "timestamp"
    timevar_ini = timestamp_ini
    timevar_end = timestamp_end
    if block_ini:
        timevar_txt = "block"
        timevar_ini = block_ini
        timevar_end = block_end

    # get all hype operations ( they are ordered by block )
    hypervisor_operations = await get_hypervisor_operations_for_twa(
        chain=chain,
        hypervisor_address=hypervisor_address,
        block_ini=block_ini,
        block_end=block_end,
        timestamp_ini=timestamp_ini,
        timestamp_end=timestamp_end,
    )
    if not hypervisor_operations:
        return f"No hypervisor operations found for {hypervisor_address}"

    result_data = {
        "hypervisor_address": hypervisor_address,
        f"{timevar_txt}_ini": timevar_ini or hypervisor_operations[0][timevar_txt],
        f"{timevar_txt}_end": timevar_end or hypervisor_operations[-1][timevar_txt],
        "total TWB": 0,  # total time weighted balances
        "total_TWB_percentage": 0,
        "total_time_passed": timevar_end - timevar_ini,
        "totalSupply_ini": None,
        "totalSupply_end": None,
        "users": {},
    }
    last_timevar = timevar_ini
    for operation in hypervisor_operations:

        # easy access vars
        # current_total_supply = operation["hypervisor_status"]["totalSupply"]
        current_timevar = operation[timevar_txt]

        # for all users already processed

        # check if user is in the dict
        if operation["user_address"] not in result_data["users"]:
            result_data["users"][operation["user_address"]] = {
                "TWB": 0,  # time weighted balance
                "initial_balance": 0,
                "final_balance": 0,  # this is the current user balance at any point
                "operations": [],
                "TWB_percentage": 0,
            }

        # if block is lower than initial block, this is the initial balance for the user
        if current_timevar < timevar_ini:
            result_data["users"][operation["user_address"]]["initial_balance"] = (
                operation["shares"]["balance"]
            )
            # temporary final balance is the same as initial balance
            result_data["users"][operation["user_address"]]["final_balance"] = (
                operation["shares"]["balance"]
            )
            # set initial total supply
            result_data["totalSupply_ini"] = operation["hypervisor_status"][
                "totalSupply"
            ]
        else:
            # this is an operation after the initial block

            # set all users time weighted balances
            for user_address, user_data in result_data["users"].items():
                if user_address == operation["user_address"]:
                    # do not calculate TWB for the user in the current operation
                    continue
                elif user_data["final_balance"] == 0:
                    # if final balance is zero, do not calculate TWB
                    continue

                _operation_to_append = {
                    f"{timevar_txt}": operation[timevar_txt],
                    "time_passed": current_timevar - last_timevar,
                    "TWB": (current_timevar - last_timevar)
                    * user_data["final_balance"],
                }
                if _operation_to_append["TWB"] != 0:
                    user_data["operations"].append(_operation_to_append)

                    # add to users total TWB
                    user_data["TWB"] += (current_timevar - last_timevar) * user_data[
                        "final_balance"
                    ]
                    # add to hypervisor total TWB
                    result_data["total TWB"] += (
                        current_timevar - last_timevar
                    ) * user_data["final_balance"]

            # append operation to the user's operations list
            _operation_to_append = {
                f"{timevar_txt}": operation[timevar_txt],
                "time_passed": current_timevar - last_timevar,
                "TWB": (current_timevar - last_timevar)
                * result_data["users"][operation["user_address"]]["final_balance"],
            }
            if _operation_to_append["TWB"] != 0:
                result_data["users"][operation["user_address"]]["operations"].append(
                    _operation_to_append
                )

                # set user's time weighted balance
                result_data["users"][operation["user_address"]]["TWB"] += (
                    current_timevar - last_timevar
                ) * result_data["users"][operation["user_address"]]["final_balance"]

                # add to hypervisor total TWB
                result_data["total TWB"] += (
                    current_timevar - last_timevar
                ) * result_data["users"][operation["user_address"]]["final_balance"]

            # set user's final balance as current balance
            result_data["users"][operation["user_address"]]["final_balance"] = (
                operation["shares"]["balance"]
            )

            # set last timevar as current timevar
            last_timevar = current_timevar

            # set end total supply
            result_data["totalSupply_end"] = operation["hypervisor_status"][
                "totalSupply"
            ]

    # last calculation if last operation is not the end
    if last_timevar < timevar_end:
        for user_address, user_data in result_data["users"].items():
            _operation_to_append = {
                f"{timevar_txt}": timevar_end,
                "time_passed": timevar_end - last_timevar,
                "TWB": (timevar_end - last_timevar) * user_data["final_balance"],
            }
            if _operation_to_append["TWB"] != 0:
                user_data["operations"].append(_operation_to_append)

                # add to users total TWB
                user_data["TWB"] += (timevar_end - last_timevar) * user_data[
                    "final_balance"
                ]
                # add to hypervisor total TWB
                result_data["total TWB"] += (timevar_end - last_timevar) * user_data[
                    "final_balance"
                ]

    # calculate users TWB percentage
    users_to_remove = []
    for user_address, user_data in result_data["users"].items():
        user_data["TWB_percentage"] = user_data["TWB"] / result_data["total TWB"]
        result_data["total_TWB_percentage"] += user_data["TWB_percentage"]
        if user_data["TWB"] == 0:
            users_to_remove.append(user_address)

    # remove users with zero TWB
    for user_address in users_to_remove:
        result_data["users"].pop(user_address)

    # total_TWB should never be greater than end total supply * total time passed
    if (
        result_data["total TWB"]
        > result_data["totalSupply_end"] * result_data["total_time_passed"]
    ):
        logging.getLogger(__name__).error(
            f"Total TWB is greater than total supply * total time passed"
        )

    return result_data
