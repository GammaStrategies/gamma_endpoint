import asyncio
from sources.common.general.enums import Chain
from sources.mongo.bins.apps.prices import get_current_prices, get_prices
from sources.mongo.bins.helpers import global_database_helper, local_database_helper
from sources.mongo.endpoint.routers import DEPLOYED


async def user_positions_summary_worker(
    user_address: str, chains: list[Chain] | None = None
):
    details = await asyncio.gather(
        *[
            user_positions_summary_worker_chain(user_address, chain)
            for chain in chains or list(set(list([cha for pro, cha in DEPLOYED])))
        ]
    )

    # return a summary totals dict object with all data in details field
    return {
        "total_current_usd_value": sum(
            [
                sum([itm["current_usd_value"] for itm in chain_details])
                for chain_details in details
            ]
        ),
        "details": details,
    }


async def user_positions_summary_worker_chain(
    user_address: str, chain: Chain | None = None
):
    # get all user's positions within the chain and current prices
    _current_prices, _user_data = await asyncio.gather(
        get_current_prices(network=chain),
        local_database_helper(network=chain).get_grouped_user_current_status(
            user_address=user_address, chain=chain
        ),
    )
    _current_prices = {itm["address"]: itm["price"] for itm in _current_prices}

    # get prices for all operations
    _prices_to_get = [itm["price_id_token0"] for itm in _user_data] + [
        itm["price_id_token1"] for itm in _user_data
    ]
    _historic_prices = await global_database_helper().get_items_from_database(
        collection_name="usd_prices", find={"id": {"$in": _prices_to_get}}
    )
    _historic_prices = {itm["id"]: itm["price"] for itm in _historic_prices}

    # calc usd values
    for _position in _user_data:
        # convert decimals128 to floats
        _position = global_database_helper().convert_decimal_to_float(
            global_database_helper().convert_d128_to_decimal(_position)
        )
        # ease access to vars
        _current_usd_price_token0 = _current_prices[_position["info"]["token0_address"]]
        _current_usd_price_token1 = _current_prices[_position["info"]["token1_address"]]
        _token0_decimals = _position["info"]["decimals_token0"]
        _token1_decimals = _position["info"]["decimals_token1"]

        # set current position value in usd ( general current value)
        _position["current_usd_value"] = (
            (_position["last_token0"] / (10**_token0_decimals))
            * _current_usd_price_token0
        ) + (
            (_position["last_token1"] / (10**_token1_decimals))
            * _current_usd_price_token1
        )

        # set usd value per operation
        for idx, _operation in enumerate(_position["operations"]):
            _op_usd_price_token0 = _historic_prices.get(
                _position["price_id_token0"][idx],
                _current_prices[_position["info"]["token0_address"]],
            )
            _op_usd_price_token1 = _historic_prices.get(
                _position["price_id_token1"][idx],
                _current_prices[_position["info"]["token1_address"]],
            )

            _operation["usd_value"] = (
                (_operation["token0"] / (10**_token0_decimals))
                * _historic_prices[_position["price_id_token0"][idx]]
            ) + (
                (_operation["token1"] / (10**_token0_decimals))
                * _historic_prices[_position["price_id_token1"][idx]]
            )

    # return data
    return _user_data
