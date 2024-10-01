import asyncio
import logging
import time
from sources.common.database.objects.hypervisor_returns.period_yield import (
    period_yield_analyzer,
    period_yield_data,
)
from sources.common.general.enums import Chain
from ..helpers import local_database_helper, global_database_helper


# hypervisor period return builder


async def build_hype_return_analysis_from_database(
    chain: Chain,
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
    ini_block: int | None = None,
    end_block: int | None = None,
    hypervisor_address: str | None = None,
    use_latest_collection: bool = False,
) -> period_yield_analyzer | None:
    """Build a period yield analysis object from the database

    Args:
        chain (Chain): _description_
        ini_timestamp (int | None, optional): _description_. Defaults to None.
        end_timestamp (int | None, optional): _description_. Defaults to None.
        ini_block (int | None, optional): _description_. Defaults to None.
        end_block (int | None, optional): _description_. Defaults to None.
        hypervisor_address (str | None, optional): _description_. Defaults to None.
        use_latest_collection (bool, optional): Will try to fallback to the latest hypervisor return data if needed. Defaults to False.

    Returns:
        period_yield_analyzer | None: _description_
    """

    # build query
    find = {"$and": []}
    if ini_block:
        find["$and"].append({"timeframe.ini.block": {"$gte": ini_block}})
    elif ini_timestamp:
        find["$and"].append({"timeframe.ini.timestamp": {"$gte": ini_timestamp}})
    if end_block:
        find["$and"].append({"timeframe.end.block": {"$lte": end_block}})
    elif end_timestamp:
        find["$and"].append({"timeframe.end.timestamp": {"$lte": end_timestamp}})
    if hypervisor_address:
        find["$and"].append({"address": hypervisor_address})

    # get data from db and convert it to objects
    yield_items = []

    # get yield items and static hype info
    db_yield_items, hype_static, db_yield_items_latest, latest_hypervisor_returns = (
        await asyncio.gather(
            local_database_helper(network=chain).get_items_from_database(
                collection_name=("hypervisor_returns"),
                find=find,
            ),
            local_database_helper(network=chain).get_items_from_database(
                collection_name="static",
                find={"address": hypervisor_address},
            ),
            local_database_helper(network=chain).get_items_from_database(
                collection_name=("hypervisor_returns_analytic_gaps"),
                find=find,
            ),
            local_database_helper(network=chain).get_items_from_database(
                collection_name=("latest_hypervisor_returns"),
                find={"address": hypervisor_address},
            ),
        )
    )

    # check if we need to use latest collection
    if not db_yield_items and use_latest_collection:
        # use latest collection
        logging.getLogger(__name__).warning(
            f" Using latest hypervisor return data for {chain.database_name} {hypervisor_address}"
        )
        db_yield_items = db_yield_items_latest

    if latest_hypervisor_returns:
        db_yield_items += latest_hypervisor_returns

    # convert yield items to objects
    for item in db_yield_items:
        # convert decimal128 to decimal and then dict to period_yield object
        item_obj = period_yield_data()
        item_obj.from_dict(global_database_helper().convert_d128_to_decimal(item))
        yield_items.append(item_obj)

    if yield_items:
        # analyze period yield items
        analyzer = period_yield_analyzer(
            chain=chain, yield_data_list=yield_items, hypervisor_static=hype_static[0]
        )

        # return analysis
        return analyzer

    # No data found
    return None
