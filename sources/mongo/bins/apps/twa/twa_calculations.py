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
        "total_TWA": 0,  # total time weighted balances
        "total_TWA_percentage": 0,
        "total_time_passed": 0,
        "totalSupply_ini": None,
        "totalSupply_end": None,
        "users": {},
    }

    result_data["total_time_passed"] = (
        result_data[f"{timevar_txt}_end"] - result_data[f"{timevar_txt}_ini"]
    )

    last_timevar = timevar_ini
    last_total_supply = None
    for operation in hypervisor_operations:

        # easy access vars
        current_timevar = operation[timevar_txt]

        # for all users already processed

        # check if user is in the dict
        if operation["user_address"] not in result_data["users"]:
            result_data["users"][operation["user_address"]] = {
                "TWA": 0,  # time weighted average balance numerator
                "TWA_percentage": 0,
                "initial_balance": 0,
                "final_balance": 0,  # this is the current user balance at any point
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
            # set initial total supply
            result_data["totalSupply_ini"] = operation["hypervisor_status"][
                "totalSupply"
            ]
        else:
            # this is an operation after the initial block
            last_total_supply = last_total_supply or result_data["totalSupply_ini"]

            # set all users time weighted averages
            for user_address, user_data in result_data["users"].items():
                if user_address == operation["user_address"]:
                    # do not calculate TWA for the user in the current operation
                    continue
                elif user_data["final_balance"] == 0:
                    # if final balance is zero, do not calculate TWA
                    continue

                _operation_to_append = {
                    f"{timevar_txt}": operation[timevar_txt],
                    "time_passed": current_timevar - last_timevar,
                    "TWA": (current_timevar - last_timevar)
                    * (user_data["final_balance"] / last_total_supply),
                    "totalSupply": last_total_supply,
                    "balance": user_data["final_balance"],
                }
                if _operation_to_append["TWA"] != 0:
                    user_data["operations"].append(_operation_to_append)

                    # add to users total TWA
                    user_data["TWA"] += (current_timevar - last_timevar) * (
                        user_data["final_balance"] / last_total_supply
                    )
                    # calculate user's TWA percentage
                    user_data["TWA_percentage"] = (
                        user_data["TWA"] / result_data["total_time_passed"]
                    )

                    # add to hypervisor total TWA
                    result_data["total_TWA"] += (current_timevar - last_timevar) * (
                        user_data["final_balance"] / last_total_supply
                    )

            # append operation to the user's operations list
            _operation_to_append = {
                f"{timevar_txt}": operation[timevar_txt],
                "time_passed": current_timevar - last_timevar,
                "TWA": (current_timevar - last_timevar)
                * (
                    result_data["users"][operation["user_address"]]["final_balance"]
                    / last_total_supply
                ),
                "totalSupply": last_total_supply,
                "balance": result_data["users"][operation["user_address"]][
                    "final_balance"
                ],
            }
            if _operation_to_append["TWA"] != 0:
                result_data["users"][operation["user_address"]]["operations"].append(
                    _operation_to_append
                )

                # set user's time weighted balance
                result_data["users"][operation["user_address"]]["TWA"] += (
                    current_timevar - last_timevar
                ) * (
                    result_data["users"][operation["user_address"]]["final_balance"]
                    / last_total_supply
                )
                # calculate user's TWA percentage
                result_data["users"][operation["user_address"]]["TWA_percentage"] = (
                    user_data["TWA"] / result_data["total_time_passed"]
                )

                # add to hypervisor total TWA
                result_data["total_TWA"] += (current_timevar - last_timevar) * (
                    result_data["users"][operation["user_address"]]["final_balance"]
                    / last_total_supply
                )

            # set user's final balance as current balance
            result_data["users"][operation["user_address"]]["final_balance"] = (
                operation["shares"]["balance"]
            )

            last_total_supply = operation["hypervisor_status"]["totalSupply"]

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
                "TWA": (timevar_end - last_timevar)
                * (user_data["final_balance"] / last_total_supply),
                "totalSupply": last_total_supply,
                "balance": user_data["final_balance"],
            }
            if _operation_to_append["TWA"] != 0:
                user_data["operations"].append(_operation_to_append)

                # add to users total TWA
                user_data["TWA"] += (timevar_end - last_timevar) * (
                    user_data["final_balance"] / last_total_supply
                )
                # calculate user's TWA percentage
                user_data["TWA_percentage"] = (
                    user_data["TWA"] / result_data["total_time_passed"]
                )
                # add to hypervisor total TWA
                result_data["total_TWA"] += (timevar_end - last_timevar) * (
                    user_data["final_balance"] / last_total_supply
                )

    # calculate users TWA percentage
    users_to_remove = []
    for user_address, user_data in result_data["users"].items():
        # force a 1
        user_data["TWA_percentage"] = (
            user_data["TWA"] / result_data["total_time_passed"]
        )
        if user_data["TWA"] == 0:
            users_to_remove.append(user_address)
        else:
            result_data["total_TWA_percentage"] += user_data["TWA_percentage"]

    # remove users with zero TWA
    for user_address in users_to_remove:
        result_data["users"].pop(user_address)

    # total_TWA should never be greater than end total supply * total time passed
    if (
        result_data["total_TWA"]
        > result_data["totalSupply_end"] * result_data["total_time_passed"]
    ):
        logging.getLogger(__name__).error(
            f"Total TWA is greater than total supply * total time passed"
        )

    return result_data


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
        "total_time_passed": 0,
        "totalSupply_ini": None,
        "totalSupply_end": None,
        "users": {},
    }

    result_data["total_time_passed"] = (
        result_data[f"{timevar_txt}_end"] - result_data[f"{timevar_txt}_ini"]
    )
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
                "TWB_percentage": 0,
                "initial_balance": 0,
                "final_balance": 0,  # this is the current user balance at any point
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
