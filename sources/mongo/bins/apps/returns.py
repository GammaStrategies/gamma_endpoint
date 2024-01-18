import asyncio
from datetime import datetime
import time
from sources.common.database.objects.hypervisor_returns.period_yield import (
    period_yield_analyzer,
    period_yield_data,
)
from sources.common.general.enums import Chain, Protocol
from sources.common.database.collection_endpoint import database_global, database_local
from sources.common.database.common.collections_common import db_collections_common
from ..helpers import local_database_helper, global_database_helper


# hypervisor period return builder


async def build_hype_return_analysis_from_database(
    chain: Chain,
    ini_timestamp: int | None = None,
    end_timestamp: int | None = None,
    ini_block: int | None = None,
    end_block: int | None = None,
    hypervisor_address: str | None = None,
) -> period_yield_analyzer | None:
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
    db_yield_items, hype_static = await asyncio.gather(
        local_database_helper(network=chain).get_items_from_database(
            collection_name="hypervisor_returns", find=find
        ),
        local_database_helper(network=chain).get_items_from_database(
            collection_name="static",
            find={"address": hypervisor_address},
        ),
    )

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
