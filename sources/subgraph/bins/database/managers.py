import logging
import asyncio
import sys
from datetime import datetime, timezone
from sources.subgraph.bins.hypervisor import HypervisorInfo, HypervisorData
from sources.subgraph.bins.masterchef_v2 import MasterchefV2Info
from sources.subgraph.bins.hype_fees.data import FeeGrowthSnapshotData
from sources.subgraph.bins.hype_fees.fees_yield import FeesYield
from sources.subgraph.bins.hype_fees.impermanent_divergence import (
    impermanent_divergence_all,
)
from sources.subgraph.bins.toplevel import TopLevelData
from sources.subgraph.bins.enums import Chain, Protocol

from sources.subgraph.bins.config import MASTERCHEF_ADDRESSES

from sources.common.database.common.collections_common import db_collections_common

logger = logging.getLogger(__name__)


class db_collection_manager(db_collections_common):
    def __init__(
        self,
        mongo_url: str,
        db_name: str,
        db_collections: dict,
    ):
        self.db_collection_name = ""
        # self.db_name = "gamma_db_v1"

        super().__init__(
            mongo_url=mongo_url, db_name=db_name, db_collections=db_collections
        )

    async def feed_db(self, chain: Chain, protocol: Protocol):
        try:
            await self.save_items_to_database(
                data=await self.create_data(chain=chain, protocol=protocol),
                collection_name=self.db_collection_name,
            )
        except Exception:
            logger.warning(
                f" Unexpected error feeding {chain}'s {protocol} database  err:{sys.exc_info()[0]}"
            )

    async def _get_data(self, query: list[dict]):
        return await self.query_items_from_database(
            query=query, collection_name=self.db_collection_name
        )


# gamma_v1 database related


class db_static_manager(db_collection_manager):
    def __init__(self, mongo_url: str):
        # Create a dictionary of collections
        self.db_collections = {"static": {"id": True}}  # no historical data}
        # Set the database name
        self.db_name = "gamma_db_v1"

        super().__init__(
            mongo_url=mongo_url,
            db_name=self.db_name,
            db_collections=self.db_collections,
        )

        # Set the collection to static, which is the name of the collection in the database
        self.db_collection_name = "static"

    async def create_data(self, chain: Chain, protocol: Protocol) -> dict:
        """Create a dictionary of hypervisor_static database models

        Args:
            chain (str): _description_
            protocol (str): _description_

        Returns:
            dict: <hypervisor_id>:<db_data_models.hypervisor_static>
        """
        # define result var
        result = {}
        hypervisors_data = HypervisorData(protocol=protocol, chain=chain)
        # get all hypervisors & their pools data

        await hypervisors_data._get_all_data()

        for hypervisor in hypervisors_data.basics_data:
            # temporal vars
            address = hypervisor["id"]
            hypervisor_name = f'{hypervisor["pool"]["token0"]["symbol"]}-{hypervisor["pool"]["token1"]["symbol"]}-{hypervisor["pool"]["fee"]}'

            _tokens = [
                {
                    "address": hypervisor["pool"]["token0"]["id"],
                    "symbol": hypervisor["pool"]["token0"]["symbol"],
                    "position": 0,
                },
                {
                    "address": hypervisor["pool"]["token1"]["id"],
                    "symbol": hypervisor["pool"]["token1"]["symbol"],
                    "position": 1,
                },
            ]
            _pool = {
                "address": hypervisor["pool"]["id"],
                "fee": hypervisor["pool"]["fee"],
                "tokens": _tokens,
            }

            # add to result
            result[address] = {
                "id": f"{chain}_{address}",
                "chain": chain,
                "address": address,
                "symbol": hypervisor_name,
                "protocol": protocol,
                "created": hypervisor["created"],
                "pool": _pool,
            }

        return result

    async def get_hypervisors_address_list(
        self, chain: Chain, protocol: Protocol = None
    ) -> list:
        _find = {"chain": chain}
        if protocol:
            _find["protocol"] = protocol

        try:
            return await self.get_distinct_items_from_database(
                field="address",
                collection_name=self.db_collection_name,
                condition=_find,
            )
        except Exception:
            return []

    async def get_hypervisors(self, chain: Chain, protocol: Protocol = None) -> list:
        _find = {"chain": chain}
        if protocol:
            _find["protocol"] = protocol

        try:
            return await self.get_items_from_database(
                collection_name=self.db_collection_name,
                find=_find,
            )
        except Exception:
            return []


class db_returns_manager(db_collection_manager):
    """This is managing database with fee Return and Impermanent divergence data

    returns data is collected from <get_fees_yield> so it is using uncollected fees to return %
    impermanent data is collected from <get_impermanent_data>

    """

    def __init__(self, mongo_url: str):
        # Create a dictionary of collections
        self.db_collections = {
            "returns": {"id": True},
            "static": {"id": True},
            "allRewards2": {"id": True},
        }
        # Set the database name
        self.db_name = "gamma_db_v1"

        super().__init__(
            mongo_url=mongo_url,
            db_name=self.db_name,
            db_collections=self.db_collections,
        )

        # Set the collection to returns, which is the name of the collection in the database
        self.db_collection_name = "returns"
        self._max_retry = 1

    # format data to be used with mongo db
    async def create_data(
        self,
        chain: Chain,
        protocol: Protocol,
        period_days: int,
        current_timestamp: int = None,
    ) -> dict:
        """Create a dictionary of hypervisor_return database models

        Args:
            chain (str): _description_
            protocol (str): _description_
            period_days (int): _description_

        Returns:
            dict:   <hypervisor_id>:<db_data_models.hypervisor_return>
        """
        # define result var
        result = {}

        # calculate return
        fees_data = FeeGrowthSnapshotData(protocol, chain)
        await fees_data.init_time(days_ago=period_days, end_timestamp=current_timestamp)
        await fees_data.get_data()

        returns_data = {}
        for hypervisor_id, fees_data_item in fees_data.data.items():
            fees_yield = FeesYield(fees_data_item, protocol, chain)
            returns = fees_yield.calculate_returns()
            returns_data[hypervisor_id] = returns

        # calculate impermanent divergence
        imperm_data = await impermanent_divergence_all(
            protocol=protocol,
            chain=chain,
            days=period_days,
            current_timestamp=fees_data.time_range.end.timestamp,
        )

        # get block n timestamp
        block = fees_data.time_range.end.block
        timestamp = fees_data.time_range.end.timestamp

        # fee yield data process
        for k, v in returns_data.items():
            if k not in result.keys():
                # set the database unique id
                database_id = f"{chain}_{k}_{block}_{period_days}"

                result[k] = {
                    "id": database_id,
                    "chain": chain,
                    "period": period_days,
                    "address": k,
                    "block": block,
                    "timestamp": timestamp,
                    "fees": {
                        "feeApr": v.apr,
                        "feeApy": v.apy,
                        "status": v.status,
                    },
                }

        # impermanent data process
        for k, v in imperm_data.items():
            # only hypervisors with FeeYield data
            if k in result:
                # add symbol
                result[k]["symbol"] = v["symbol"]
                # add impermanent
                result[k]["impermanent"] = {
                    # "ini_block": v["ini_block"],
                    # "end_block": v["end_block"],
                    # "ini_timestamp": v["ini_timestamp"],
                    # "end_timestamp": v["end_timestamp"],
                    "lping": v["lping"],
                    "hodl_deposited": v["hodl_deposited"],
                    "hodl_fifty": v["hodl_fifty"],
                    "hodl_token0": v["hodl_token0"],
                    "hodl_token1": v["hodl_token1"],
                }

        return result

    async def feed_db(
        self,
        chain: Chain,
        protocol: Protocol,
        periods: list[int] = None,
        retried: int = 0,
        current_timestamp: int = None,
    ):
        """
        Args:
            chain (Chain):
            protocol (Protocol):
            periods (list[int], optional): . Defaults to [1, 7, 14, 30].
            retried (int, optional): current number of retries . Defaults to 0.
        """
        # set default periods
        if not periods:
            periods = [1, 7, 14, 30]

        # create data
        try:
            requests = [
                self.save_items_to_database(
                    data=await self.create_data(
                        chain=chain,
                        protocol=protocol,
                        period_days=days,
                        current_timestamp=current_timestamp,
                    ),
                    collection_name=self.db_collection_name,
                )
                for days in periods
            ]

            await asyncio.gather(*requests)

        except Exception as err:
            # retry when possible
            if retried < self._max_retry:
                # wait jic
                await asyncio.sleep(2)
                logger.info(
                    f" Retrying the feeding of {chain}'s {protocol} returns to db for the {retried+1} time."
                )
                # retry
                await self.feed_db(
                    chain=chain,
                    protocol=protocol,
                    periods=periods,
                    retried=retried + 1,
                    current_timestamp=current_timestamp,
                )
            elif err:
                # {'message': 'Failed to decode `block.number` value: `subgraph QmXUphAvAEiGcTzdopmaEt8YDxZ2uEmLJcCQGcfaDvRhp2 only has data starting at block number 63562887 and data for block number 50084142 is therefore not available`'}
                logger.debug(
                    f" Can't feed database {chain}'s {protocol} returns to db  err:{err.args[0]}. Retries: {retried}."
                )
            else:
                logger.exception(
                    f" Unexpected error feeding {chain}'s {protocol} returns to db  err:{sys.exc_info()[0]}. Retries: {retried}."
                )

    async def get_hypervisors_average(
        self, chain: Chain, period: int = 0, protocol: Protocol = ""
    ) -> dict:
        result = await self._get_data(
            query=self.query_hypervisors_average(
                chain=chain, period=period, protocol=protocol
            )
        )
        try:
            return result
        except Exception:
            return {}

    async def get_hypervisors_returns_average(
        self, chain: Chain, period: int = 0, protocol: Protocol = ""
    ) -> dict:
        result = await self._get_data(
            query=self.query_hypervisors_returns_average(
                chain=chain, period=period, protocol=protocol
            )
        )
        try:
            return result
        except Exception:
            return {}

    async def get_hypervisor_average(
        self,
        chain: Chain,
        hypervisor_address: str,
        period: int = 0,
        protocol: Protocol = "",
    ) -> dict:
        result = await self._get_data(
            query=self.query_hypervisors_average(
                chain=chain,
                hypervisor_address=hypervisor_address,
                period=period,
                protocol=protocol,
            )
        )
        try:
            return result
        except Exception:
            return {}

    async def get_feeReturns(
        self,
        chain: Chain,
        protocol: Protocol,
        period: int,
        hypervisor_address: str = "",
    ) -> dict:
        # query database
        dbdata = await self._get_data(
            query=self.query_last_returns(
                chain=chain,
                protocol=protocol,
                period=period,
                hypervisor_address=hypervisor_address,
            )
        )
        # set database last update field as the maximum date found within the items returned
        try:
            db_lastUpdate = max(x["timestamp"] for x in dbdata)
        except Exception:
            # TODO: log error
            db_lastUpdate = datetime.now(timezone.utc).timestamp()

        # init result
        result = {}
        # convert result to dict
        for item in dbdata:
            address = item.pop("address")
            result[address] = item

        # add database last update datetime
        result["datetime"] = datetime.fromtimestamp(db_lastUpdate)

        return result

    async def get_returns(
        self, chain: Chain, protocol: Protocol, hypervisor_address: str = ""
    ) -> dict:
        # query database
        result = await self._get_data(
            query=self.query_last_returns(
                chain=chain,
                protocol=protocol,
                hypervisor_address=hypervisor_address,
            )
        )
        # set database last update field as the maximum date found within the items returned
        try:
            db_lastUpdate = max(x["timestamp"] for x in result)
        except Exception:
            # TODO: log error
            db_lastUpdate = datetime.now(timezone.utc).timestamp()

        # convert result to dict
        result = {
            x["_id"]: {
                "daily": x["daily"],
                "weekly": x["weekly"],
                "monthly": x["monthly"],
                "allTime": x["allTime"],
            }
            for x in result
        }
        result["datetime"] = datetime.fromtimestamp(db_lastUpdate)

        return result

    async def get_impermanentDivergence_data(
        self,
        chain: Chain,
        protocol: Protocol,
        period: int,
    ) -> dict:
        # query database
        dbdata = await self._get_data(
            query=self.query_impermanentDivergence(
                chain=chain,
                protocol=protocol,
                period=period,
            )
        )
        # set database last update field as the maximum date found within the items returned
        try:
            db_lastUpdate = max([x["timestamp"] for x in dbdata])
        except Exception:
            # TODO: log error
            db_lastUpdate = datetime.utcnow().timestamp()

        # init result
        result = dict()
        # convert result to dict
        for item in dbdata:
            address = item.pop("address")
            result[address] = {
                "id": address,
                "symbol": item["symbol"],
                "lping": item["lping"],
                "hodl_deposited": item["hodl_deposited"],
                "hodl_fifty": item["hodl_fifty"],
                "hodl_token0": item["hodl_token0"],
                "hodl_token1": item["hodl_token1"],
            }

        # add database last update datetime
        result["datetime"] = datetime.fromtimestamp(db_lastUpdate)

        return result

    async def get_analytics_data(
        self,
        chain: Chain,
        hypervisor_address: str,
        period: int,
        ini_date: datetime,
        end_date: datetime,
    ) -> list:
        return await self._get_data(
            query=self.query_return_imperm_rewards2_flat(
                chain=chain,
                hypervisor_address=hypervisor_address,
                period=period,
                ini_date=ini_date,
                end_date=end_date,
            )
        )

    async def get_analytics_data_variation(
        self,
        chain: Chain,
        hypervisor_address: str,
        period: int,
        ini_date: datetime,
        end_date: datetime,
    ) -> list:
        result = []
        last_row = None
        for idx, row in enumerate(
            await self._get_analytics_data(
                chain=chain,
                hypervisor_address=hypervisor_address,
                period=period,
                ini_date=ini_date,
                end_date=end_date,
            )
        ):
            if idx == 0:
                result.append({k: 0 for k, v in row.items()})
            else:
                result.append({k: v - last_row[k] for k, v in row.items()})
            last_row = row
        return result

    @staticmethod
    def query_hypervisors_average(
        chain: Chain,
        period: int = 0,
        protocol: Protocol = "",
        hypervisor_address: str = "",
    ) -> list[dict]:
        """get all average returns from collection

        Args:
            chain (str): _description_
            period (int, optional): _description_. Defaults to 0.
            protocol (str)
            hypervisor_address (str)

        Returns:
            list[dict]:
                { "_id" = hypervisor address, "hipervisor":{ ... }, "periods": { ... }  }

        """
        # set return match vars
        _returns_match = {"chain": chain}

        if period != 0:
            _returns_match["period"] = period
        if hypervisor_address != "":
            _returns_match["address"] = hypervisor_address

        # set return match vars
        _static_match = {}
        if protocol != "":
            _static_match["hypervisor.protocol"] = protocol

        # return query
        return [
            {"$match": _returns_match},
            {
                "$project": {
                    "period": "$period",
                    "address": "$address",
                    "hypervisor_id": {"$concat": ["$chain", "_", "$address"]},
                    "timestamp": "$timestamp",
                    "block": "$block",
                    "feeApr": "$fees.feeApr",
                    "feeApy": "$fees.feeApy",
                    "imp_vs_hodl_usd": "$impermanent.vs_hodl_usd",
                    "imp_vs_hodl_deposited": "$impermanent.vs_hodl_deposited",
                    "imp_vs_hodl_token0": "$impermanent.vs_hodl_token0",
                    "imp_vs_hodl_token1": "$impermanent.vs_hodl_token1",
                }
            },
            {
                "$lookup": {
                    "from": "static",
                    "localField": "hypervisor_id",
                    "foreignField": "id",
                    "as": "hypervisor",
                }
            },
            {"$set": {"hypervisor": {"$arrayElemAt": ["$hypervisor", 0]}}},
            {"$match": _static_match},
            {"$sort": {"block": 1}},
            {
                "$project": {
                    "period": "$period",
                    "address": "$address",
                    "timestamp": "$timestamp",
                    "block": "$block",
                    "feeApr": "$feeApr",
                    "feeApy": "$feeApy",
                    "imp_vs_hodl_usd": "$imp_vs_hodl_usd",
                    "imp_vs_hodl_deposited": "$imp_vs_hodl_deposited",
                    "imp_vs_hodl_token0": "$imp_vs_hodl_token0",
                    "imp_vs_hodl_token1": "$imp_vs_hodl_token1",
                    "hypervisor": "$hypervisor",
                }
            },
            {
                "$group": {
                    "_id": {"address": "$address", "period": "$period"},
                    "min_timestamp": {"$min": "$timestamp"},
                    "max_timestamp": {"$max": "$timestamp"},
                    "min_block": {"$min": "$block"},
                    "max_block": {"$max": "$block"},
                    "av_feeApr": {"$avg": "$feeApr"},
                    "av_feeApy": {"$avg": "$feeApy"},
                    "av_imp_vs_hodl_usd": {"$avg": "$imp_vs_hodl_usd"},
                    "av_imp_vs_hodl_deposited": {"$avg": "$imp_vs_hodl_deposited"},
                    "av_imp_vs_hodl_token0": {"$avg": "$imp_vs_hodl_token0"},
                    "av_imp_vs_hodl_token1": {"$avg": "$imp_vs_hodl_token1"},
                    "hypervisor": {"$first": "$hypervisor"},
                }
            },
            {
                "$group": {
                    "_id": "$_id.address",
                    "periods": {
                        "$push": {
                            "k": {"$toString": "$_id.period"},
                            "v": {
                                "period": "$_id.period",
                                "items": "$items",
                                "min_timestamp": "$min_timestamp",
                                "max_timestamp": "$max_timestamp",
                                "min_block": "$min_block",
                                "max_block": "$max_block",
                                "av_feeApr": "$av_feeApr",
                                "av_feeApy": "$av_feeApy",
                                "av_imp_vs_hodl_usd": "$av_imp_vs_hodl_usd",
                                "av_imp_vs_hodl_deposited": "$av_imp_vs_hodl_deposited",
                                "av_imp_vs_hodl_token0": "$av_imp_vs_hodl_token0",
                                "av_imp_vs_hodl_token1": "$av_imp_vs_hodl_token1",
                            },
                        },
                    },
                    "hypervisor": {"$first": "$hypervisor"},
                }
            },
            {
                "$project": {
                    "_id": "$_id",
                    "hypervisor": {
                        "symbol": "$hypervisor.symbol",
                        "address": "$hypervisor.address",
                        "chain": "$hypervisor.chain",
                        "pool": "$hypervisor.pool",
                        "protocol": "$hypervisor.protocol",
                    },
                    "returns": {"$arrayToObject": "$periods"},
                }
            },
        ]

    @staticmethod
    def query_hypervisors_returns_average(
        chain: Chain,
        period: int = 0,
        protocol: Protocol = "",
        hypervisor_address: str = "",
    ) -> list[dict]:
        """get all average returns from collection

        Args:
            chain (str): _description_
            period (int, optional): _description_. Defaults to 0.
            protocol (str)
            hypervisor_address (str)

        Returns:
            list[dict]:
                { "_id" = hypervisor address, "hipervisor":{ ... }, "periods": { ... }  }

        """
        # set return match vars
        _returns_match = {
            "chain": chain,
            "$and": [{"fees.feeApy": {"$gt": 0}}, {"fees.feeApy": {"$lt": 8}}],
        }

        if period != 0:
            _returns_match["period"] = period
        if hypervisor_address != "":
            _returns_match["address"] = hypervisor_address

        # set return match vars
        _static_match = {}
        if protocol != "":
            _static_match["hypervisor.protocol"] = protocol

        # return query
        return [
            {"$match": _returns_match},
            {
                "$project": {
                    "period": "$period",
                    "address": "$address",
                    "hypervisor_id": {"$concat": ["$chain", "_", "$address"]},
                    "timestamp": "$timestamp",
                    "block": "$block",
                    "feeApr": "$fees.feeApr",
                    "feeApy": "$fees.feeApy",
                    "imp_vs_hodl_usd": "$impermanent.vs_hodl_usd",
                    "imp_vs_hodl_deposited": "$impermanent.vs_hodl_deposited",
                    "imp_vs_hodl_token0": "$impermanent.vs_hodl_token0",
                    "imp_vs_hodl_token1": "$impermanent.vs_hodl_token1",
                }
            },
            {
                "$lookup": {
                    "from": "static",
                    "localField": "hypervisor_id",
                    "foreignField": "id",
                    "as": "hypervisor",
                }
            },
            {"$set": {"hypervisor": {"$arrayElemAt": ["$hypervisor", 0]}}},
            {"$match": _static_match},
            {"$sort": {"block": 1}},
            {
                "$project": {
                    "period": "$period",
                    "address": "$address",
                    "timestamp": "$timestamp",
                    "block": "$block",
                    "feeApr": "$feeApr",
                    "feeApy": "$feeApy",
                    "hypervisor": "$hypervisor",
                }
            },
            {
                "$group": {
                    "_id": {"address": "$address", "period": "$period"},
                    "min_timestamp": {"$min": "$timestamp"},
                    "max_timestamp": {"$max": "$timestamp"},
                    "min_block": {"$min": "$block"},
                    "max_block": {"$max": "$block"},
                    "av_feeApr": {"$avg": "$feeApr"},
                    "av_feeApy": {"$avg": "$feeApy"},
                    "hypervisor": {"$first": "$hypervisor"},
                }
            },
            {
                "$group": {
                    "_id": "$_id.address",
                    "periods": {
                        "$push": {
                            "k": {"$toString": "$_id.period"},
                            "v": {
                                "period": "$_id.period",
                                "items": "$items",
                                "min_timestamp": "$min_timestamp",
                                "max_timestamp": "$max_timestamp",
                                "min_block": "$min_block",
                                "max_block": "$max_block",
                                "av_feeApr": "$av_feeApr",
                                "av_feeApy": "$av_feeApy",
                            },
                        },
                    },
                    "hypervisor": {"$first": "$hypervisor"},
                }
            },
            {
                "$project": {
                    "_id": "$_id",
                    "hypervisor": {
                        "symbol": "$hypervisor.symbol",
                        "address": "$hypervisor.address",
                        "chain": "$hypervisor.chain",
                        "pool": "$hypervisor.pool",
                        "protocol": "$hypervisor.protocol",
                    },
                    "returns": {"$arrayToObject": "$periods"},
                }
            },
        ]

    @staticmethod
    def query_last_returns(
        chain: Chain,
        period: int = 0,
        protocol: Protocol = "",
        hypervisor_address: str = "",
    ) -> list[dict]:
        """return the last items found not zero lower than 800% apy apr :
                daily, weekly and monthly apr apy ( alltime is the monthly figure)

        Args:
            chain (str):
            period (int, optional): . Defaults to 0.
            protocol (str, optional): . Defaults to "".
            hypervisor_address (str, optional): . Defaults to "".

        Returns:
            list[dict]:
                        when period == default {
                                                "_id" : "0xeb7d263db66aab4d5ee903a949a5a54c287bec87",
                                                "daily" : {
                                                    "feeApr" : 0.0173442096430378,
                                                    "feeApy" : 0.017495074535651,
                                                    "hasOutlier" : "False",
                                                    "symbol" : "WMATIC-stMATIC-0"
                                                },
                                                "weekly" : {
                                                    "feeApr" : 0.00174322708835021,
                                                    "feeApy" : 0.00174474322190754,
                                                    "hasOutlier" : "False",
                                                    "symbol" : "WMATIC-stMATIC-0"
                                                },
                                                "monthly" : {
                                                    "feeApr" : 0.00134238749591191,
                                                    "feeApy" : 0.00134328642948756,
                                                    "hasOutlier" : "False",
                                                    "symbol" : "WMATIC-stMATIC-0"
                                                },
                                                "allTime" : {
                                                    "feeApr" : 0.00134238749591191,
                                                    "feeApy" : 0.00134328642948756,
                                                    "hasOutlier" : "False",
                                                    "symbol" : "WMATIC-stMATIC-0"
                                                }
                                            }

                        when period != 0 {
                                        "address" : "0xf874d4957861e193aec9937223062679c14f9aca",
                                        "timestamp" : 1675329215,
                                        "block" : 38817275,
                                        "feeApr" : 0.0560324909858921,
                                        "feeApy" : 0.0576274984164038,
                                        "hasOutlier" : "False",
                                        "symbol" : "WMATIC-WETH-500"
                                        }
        """

        # set return match vars
        _returns_match = {
            "chain": chain,
            "$and": [{"fees.feeApr": {"$gt": 0}}, {"fees.feeApr": {"$lt": 9}}],
            "$and": [{"fees.feeApy": {"$gt": 0}}, {"fees.feeApy": {"$lt": 9}}],
            "$and": [{"fees.feeApr": {"$gt": 0}}, {"fees.feeApr": {"$lt": 9}}],
            "$and": [{"fees.feeApy": {"$gt": 0}}, {"fees.feeApy": {"$lt": 9}}],
        }

        if period != 0:
            _returns_match["period"] = period
        if hypervisor_address != "":
            _returns_match["address"] = hypervisor_address

        # set return match vars
        _static_match = {}
        if protocol != "":
            _static_match["hypervisor.protocol"] = protocol

        # will return a list of:
        # {
        #     "_id" : "0xeb7d263db66aab4d5ee903a949a5a54c287bec87",
        #     "daily" : {
        #         "feeApr" : 0.0173442096430378,
        #         "feeApy" : 0.017495074535651,
        #         "hasOutlier" : "False",
        #         "symbol" : "WMATIC-stMATIC-0"
        #     },
        #     "weekly" : {
        #         "feeApr" : 0.00174322708835021,
        #         "feeApy" : 0.00174474322190754,
        #         "hasOutlier" : "False",
        #         "symbol" : "WMATIC-stMATIC-0"
        #     },
        #     "monthly" : {
        #         "feeApr" : 0.00134238749591191,
        #         "feeApy" : 0.00134328642948756,
        #         "hasOutlier" : "False",
        #         "symbol" : "WMATIC-stMATIC-0"
        #     },
        #     "allTime" : {
        #         "feeApr" : 0.00134238749591191,
        #         "feeApy" : 0.00134328642948756,
        #         "hasOutlier" : "False",
        #         "symbol" : "WMATIC-stMATIC-0"
        #     }
        # }
        returns_all_periods = [
            {"$match": _returns_match},
            {
                "$project": {
                    "period": "$period",
                    "address": "$address",
                    "hypervisor_id": {"$concat": ["$chain", "_", "$address"]},
                    "timestamp": "$timestamp",
                    "block": "$block",
                    "feeApr": "$fees.feeApr",
                    "feeApy": "$fees.feeApy",
                    "status": "$fees.status",
                }
            },
            {
                "$lookup": {
                    "from": "static",
                    "localField": "hypervisor_id",
                    "foreignField": "id",
                    "as": "hypervisor",
                }
            },
            {"$set": {"hypervisor": {"$arrayElemAt": ["$hypervisor", 0]}}},
            {"$match": _static_match},
            {"$sort": {"block": 1}},
            {
                "$project": {
                    "period": "$period",
                    "address": "$address",
                    "timestamp": "$timestamp",
                    "block": "$block",
                    "feeApr": "$feeApr",
                    "feeApy": "$feeApy",
                    "status": "$status",
                    "symbol": "$hypervisor.symbol",
                }
            },
            {
                "$group": {
                    "_id": {"address": "$address", "period": "$period"},
                    "items": {"$push": "$$ROOT"},
                }
            },
            {
                "$group": {
                    "_id": "$_id.address",
                    "periods": {
                        "$push": {
                            "k": {"$toString": "$_id.period"},
                            "v": {"$last": "$items"},
                        },
                    },
                }
            },
            {
                "$project": {
                    "_id": "$_id",
                    "returns": {"$arrayToObject": "$periods"},
                }
            },
            {
                "$addFields": {
                    "daily": {
                        "feeApr": "$returns.1.feeApr",
                        "feeApy": "$returns.1.feeApy",
                        "status": "$returns.1.status",
                        "symbol": "$returns.1.symbol",
                    },
                    "weekly": {
                        "feeApr": "$returns.7.feeApr",
                        "feeApy": "$returns.7.feeApy",
                        "status": "$returns.7.status",
                        "symbol": "$returns.7.symbol",
                    },
                    "monthly": {
                        "feeApr": "$returns.30.feeApr",
                        "feeApy": "$returns.30.feeApy",
                        "status": "$returns.30.status",
                        "symbol": "$returns.30.symbol",
                    },
                    "allTime": {
                        "feeApr": "$returns.30.feeApr",
                        "feeApy": "$returns.30.feeApy",
                        "status": "$returns.30.status",
                        "symbol": "$returns.30.symbol",
                    },
                }
            },
            {"$unset": ["returns"]},
        ]

        # will return a list of {
        #     "address" : "0xf874d4957861e193aec9937223062679c14f9aca",
        #     "timestamp" : 1675329215,
        #     "block" : 38817275,
        #     "feeApr" : 0.0560324909858921,
        #     "feeApy" : 0.0576274984164038,
        #     "hasOutlier" : "False",
        #     "symbol" : "WMATIC-WETH-500"
        # }
        returns_by_period = [
            {"$match": _returns_match},
            {
                "$project": {
                    "address": "$address",
                    "hypervisor_id": {"$concat": ["$chain", "_", "$address"]},
                    "timestamp": "$timestamp",
                    "feeApr": "$fees.feeApr",
                    "feeApy": "$fees.feeApy",
                    "status": "$fees.status",
                    "block": "$block",
                }
            },
            {
                "$lookup": {
                    "from": "static",
                    "localField": "hypervisor_id",
                    "foreignField": "id",
                    "as": "hypervisor",
                }
            },
            {"$set": {"hypervisor": {"$arrayElemAt": ["$hypervisor", 0]}}},
            {"$match": _static_match},
            {"$sort": {"block": -1}},
            {
                "$project": {
                    "address": "$address",
                    "timestamp": "$timestamp",
                    "block": "$block",
                    "feeApr": "$feeApr",
                    "feeApy": "$feeApy",
                    "status": "$status",
                    "symbol": "$hypervisor.symbol",
                }
            },
            {
                "$group": {
                    "_id": "$address",
                    "items": {"$first": "$$ROOT"},
                }
            },
            {"$replaceRoot": {"newRoot": "$items"}},
            {"$unset": ["_id"]},
        ]

        return returns_by_period if period != 0 else returns_all_periods

    @staticmethod
    def query_return_impermanent(
        chain: Chain,
        period: int = 0,
        protocol: Protocol = None,
        hypervisor_address: str = None,
        ini_date: datetime = None,
        end_date: datetime = None,
    ) -> list[dict]:
        # build first main match part of the query
        _match = {"chain": chain, "period": period}
        if hypervisor_address:
            _match["address"] = hypervisor_address
        if ini_date and end_date:
            _match["$and"] = [
                {"timestamp": {"$gte": int(ini_date.timestamp())}},
                {"timestamp": {"$lte": int(end_date.timestamp())}},
            ]
        elif ini_date:
            _match["timestamp"] = {"$gte": int(ini_date.timestamp())}
        elif end_date:
            _match["timestamp"] = {"$lte": int(end_date.timestamp())}

        _query = [{"$match": _match}]
        # build protocol part as needed
        if protocol:
            _query.extend(
                (
                    {
                        "$lookup": {
                            "from": "static",
                            "localField": "hypervisor_id",
                            "foreignField": "id",
                            "as": "hypervisor",
                        }
                    },
                    {"$set": {"hypervisor": {"$arrayElemAt": ["$hypervisor", 0]}}},
                    {"$match": {"hypervisor.protocol": protocol}},
                )
            )

        _query.extend(
            (
                {"$sort": {"timestamp": -1}},
                {"$unset": ["_id", "hypervisor_id", "hypervisor", "id"]},
            )
        )

        # debug_query = f"{_query}"

        # return result
        return _query

    @staticmethod
    def query_return_imperm_rewards2_flat(
        chain: Chain,
        period: int,
        hypervisor_address: str,
        ini_date: datetime = None,
        end_date: datetime = None,
    ) -> list[dict]:
        """
            matches the first lte return timestamp rewards2 measure and adds it

        Args:
            chain (Chain):
            period (int):
            hypervisor_address (str):
            ini_date (datetime, optional): . Defaults to None.
            end_date (datetime, optional): . Defaults to None.

        Returns:
            list[dict]: query
        """

        # build first main match part of the query
        _match = {"chain": chain, "period": period, "address": hypervisor_address}

        if ini_date and end_date:
            _match["$and"] = [
                {"timestamp": {"$gte": int(ini_date.timestamp())}},
                {"timestamp": {"$lte": int(end_date.timestamp())}},
            ]
        elif ini_date:
            _match["timestamp"] = {"$gte": int(ini_date.timestamp())}
        elif end_date:
            _match["timestamp"] = {"$lte": int(end_date.timestamp())}

        # construct the allRewards2 match part of the query ( filter masterchefs addresses)
        _allrewards2_match = {}
        valid_masterchefs = [
            {"obj_as_arr.k": address.lower()}
            for dex, address_list in MASTERCHEF_ADDRESSES.get(chain, {}).items()
            for address in address_list
        ]
        if valid_masterchefs:
            _allrewards2_match = {
                "$and": [
                    {"$or": valid_masterchefs},
                    {f"obj_as_arr.v.pools.{hypervisor_address}": {"$exists": 1}},
                ]
            }
        else:
            _allrewards2_match = {
                f"obj_as_arr.v.pools.{hypervisor_address}": {"$exists": 1}
            }

        # allrewards2 subquery: pick the sum of each rewarder apr
        year_allRewards2_subquery = {
            "$ifNull": [
                {"$sum": f"$allRewards2.obj_as_arr.v.pools.{hypervisor_address}.apr"},
                0,
            ]
        }

        # return result
        _query = [
            {"$match": _match},
            {
                "$lookup": {
                    "from": "allRewards2",
                    "let": {
                        "returns_chain": "$chain",
                        "returns_datetime": {
                            "$toDate": {"$multiply": ["$timestamp", 1000]}
                        },
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$chain", "$$returns_chain"]},
                                        {
                                            "$lte": [
                                                "$datetime",
                                                "$$returns_datetime",
                                            ]
                                        },
                                    ],
                                }
                            }
                        },
                        {"$sort": {"datetime": -1}},
                        {"$limit": 1},
                        {"$addFields": {"obj_as_arr": {"$objectToArray": "$$ROOT"}}},
                        {"$unwind": "$obj_as_arr"},
                        {"$match": _allrewards2_match},
                    ],
                    "as": "allRewards2",
                }
            },
            {"$sort": {"timestamp": 1}},
            {
                "$project": {
                    "chain": "$chain",
                    "address": "$address",
                    "symbol": "$symbol",
                    "block": "$block",
                    "timestamp": "$timestamp",
                    "period": "$period",
                    "year_feeApr": "$fees.feeApr",
                    "year_feeApy": "$fees.feeApy",
                    "year_allRewards2": year_allRewards2_subquery,
                    "period_feeApr": {
                        "$multiply": ["$period", {"$divide": ["$fees.feeApr", 365]}]
                    },
                    "period_rewardsApr": {
                        "$multiply": [
                            "$period",
                            {
                                "$divide": [
                                    year_allRewards2_subquery,
                                    365,
                                ]
                            },
                        ]
                    },
                    "period_lping": "$impermanent.lping",
                    "period_hodl_deposited": "$impermanent.hodl_deposited",
                    "period_hodl_fifty": "$impermanent.hodl_fifty",
                    "period_hodl_token0": "$impermanent.hodl_token0",
                    "period_hodl_token1": "$impermanent.hodl_token1",
                }
            },
            {
                "$addFields": {
                    "period_netApr": {"$sum": ["$period_lping", "$period_rewardsApr"]},
                    "period_impermanentResult": {
                        "$subtract": ["$period_lping", "$period_feeApr"]
                    },
                }
            },
            {
                "$addFields": {
                    "gamma_vs_hodl": {
                        "$subtract": [
                            {
                                "$divide": [
                                    {"$sum": ["$period_netApr", 1]},
                                    {"$sum": ["$period_hodl_deposited", 1]},
                                ]
                            },
                            1,
                        ]
                    },
                }
                ##### FILTER: exclude big differences btwen gamma and deposited ####
            },
            {
                "$addFields": {
                    "exclude": {
                        "$abs": {
                            "$subtract": ["$gamma_vs_hodl", "$period_hodl_deposited"]
                        }
                    }
                }
            },
            {"$match": {"exclude": {"$lte": 0.2}}},
            {"$unset": ["_id", "exclude"]},
        ]

        # debug_query = f"{_query}"
        return _query

    @staticmethod
    def query_impermanentDivergence(
        chain: Chain, protocol: Protocol, period: int
    ) -> list[dict]:
        # set return match vars
        _returns_match = {"chain": chain, "period": period}
        # set protocol match vars
        _static_match = {"hypervisor.protocol": protocol}
        # set query
        return [
            {"$match": _returns_match},
            {
                "$project": {
                    "period": "$period",
                    "address": "$address",
                    "hypervisor_id": {"$concat": ["$chain", "_", "$address"]},
                    "timestamp": "$timestamp",
                    "block": "$block",
                    "lping": "$impermanent.lping",
                    "hodl_deposited": "$impermanent.hodl_deposited",
                    "hodl_fifty": "$impermanent.hodl_fifty",
                    "hodl_token0": "$impermanent.hodl_token0",
                    "hodl_token1": "$impermanent.hodl_token1",
                }
            },
            {
                "$lookup": {
                    "from": "static",
                    "localField": "hypervisor_id",
                    "foreignField": "id",
                    "as": "hypervisor",
                }
            },
            {"$set": {"hypervisor": {"$arrayElemAt": ["$hypervisor", 0]}}},
            {"$match": _static_match},
            {"$sort": {"block": -1}},
            {
                "$project": {
                    "period": "$period",
                    "address": "$address",
                    "timestamp": "$timestamp",
                    "block": "$block",
                    "lping": "$lping",
                    "hodl_deposited": "$hodl_deposited",
                    "hodl_fifty": "$hodl_fifty",
                    "hodl_token0": "$hodl_token0",
                    "hodl_token1": "$hodl_token1",
                    "symbol": "$hypervisor.symbol",
                }
            },
            {
                "$group": {
                    "_id": {"address": "$address", "period": "$period"},
                    "item": {"$first": "$$ROOT"},
                }
            },
            {"$replaceRoot": {"newRoot": "$item"}},
            {"$unset": ["_id"]},
        ]


class db_allData_manager(db_collection_manager):
    def __init__(self, mongo_url: str):
        # Create a dictionary of collections
        self.db_collections = {"allData": {"id": True}}
        # Set the database name
        self.db_name = "gamma_db_v1"

        super().__init__(
            mongo_url=mongo_url,
            db_name=self.db_name,
            db_collections=self.db_collections,
        )
        self.db_collection_name = "allData"

    async def create_data(self, chain: Chain, protocol: Protocol) -> dict:
        """Create a dictionary of hypervisor_allData database models

        Args:
            chain (str): _description_
            protocol (str): _description_

        Returns:
            dict: <hypervisor_id>:<db_data_models.hypervisor_static>
        """
        # define result var
        result = {}
        hypervisor_info = HypervisorInfo(protocol=protocol, chain=chain)
        allData = await hypervisor_info.all_data()

        # types conversion
        for hyp_id, hypervisor in allData.items():
            hypervisor["totalSupply"] = str(hypervisor["totalSupply"])
            hypervisor["maxTotalSupply"] = str(hypervisor["maxTotalSupply"])
            # hypervisor["id"] = hyp_id

        # add id and datetime to data
        allData["id"] = f"{chain}_{protocol}"
        allData["datetime"] = datetime.now(timezone.utc)

        return allData

    async def feed_db(self, chain: Chain, protocol: Protocol):
        try:
            # save as 1 item ( not separated)
            await self.save_item_to_database(
                data=await self.create_data(chain=chain, protocol=protocol),
                collection_name=self.db_collection_name,
            )
        except Exception:
            logger.warning(
                f" Unexpected error feeding  {chain}'s {protocol} allData to db   err:{sys.exc_info()[0]}"
            )

    async def get_data(self, chain: Chain, protocol: Protocol) -> dict:
        result = await self._get_data(
            query=self.query_all(chain=chain, protocol=protocol)
        )
        try:
            return result[0]
        except Exception:
            return {}

    @staticmethod
    def query_all(chain: Chain, protocol: Protocol = "") -> list[dict]:
        """
        Args:
            chain (str): _description_
            protocol (str)

        Returns:
            list[dict]:

        """
        # set return match vars
        _match = {"id": f"{chain}_{protocol}"}

        # return query
        return [{"$match": _match}, {"$unset": ["_id", "id"]}]


class db_allRewards2_manager(db_collection_manager):
    def __init__(self, mongo_url: str):
        # Create a dictionary of collections
        self.db_collections = {"allRewards2": {"id": True}}
        # Set the database name
        self.db_name = "gamma_db_v1"

        super().__init__(
            mongo_url=mongo_url,
            db_name=self.db_name,
            db_collections=self.db_collections,
        )

        # Set the collection, which is the name of the collection in the database
        self.db_collection_name = "allRewards2"

    async def create_data(self, chain: Chain, protocol: Protocol) -> dict:
        """

        Args:
            chain (str): _description_
            protocol (str): _description_

        Returns:
            dict:
        """
        # define result var
        data = {}
        try:
            masterchef_info = MasterchefV2Info(protocol=protocol, chain=chain)
            data = await masterchef_info.output(get_data=True)
        except Exception as e:
            # some pools do not have Masterchef info
            raise ValueError(
                f" {chain}'s {protocol} has no Masterchef v2 implemented "
            ) from e

        # add id and datetime to data
        data["datetime"] = datetime.now(timezone.utc)
        # get timestamp without decimals
        timestamp = int(datetime.timestamp(data["datetime"]))
        # set id
        data["id"] = f"{timestamp}_{chain}_{protocol}"
        # identify data
        data["chain"] = chain
        data["protocol"] = protocol

        return data

    async def feed_db(self, chain: Chain, protocol: Protocol):
        try:
            # save as 1 item ( not separated)
            await self.save_item_to_database(
                data=await self.create_data(chain=chain, protocol=protocol),
                collection_name=self.db_collection_name,
            )
        except ValueError:
            pass
        except Exception:
            logger.warning(
                f" Unexpected error feeding  {chain}'s {protocol} allRewards2 to db   err:{sys.exc_info()[0]}"
            )

    async def get_data(self, chain: Chain, protocol: Protocol) -> dict:
        result = await self._get_data(
            query=self.query_all(chain=chain, protocol=protocol)
        )
        try:
            return result[0]
        except Exception:
            return {}

    async def get_last_data(self, chain: Chain, protocol: Protocol) -> dict:
        """Retrieve last chain+protocol data available at database

        Args:
            chain (str):
            protocol (str):

        Returns:
            dict:
        """
        result = await self._get_data(
            query=self.query_last(chain=chain, protocol=protocol)
        )

        try:
            return result[0]
        except Exception:
            return {}

    async def get_hypervisor_rewards(
        self,
        chain: Chain,
        address: str,
        ini_date: datetime = None,
        end_date: datetime = None,
    ) -> list[dict]:
        try:
            return await self._get_data(
                query=self.query_hype_rewards(
                    chain=chain,
                    hypervisor_address=address,
                    ini_date=ini_date,
                    end_date=end_date,
                )
            )
        except Exception:
            return list({})

    @staticmethod
    def query_all(chain: Chain, protocol: Protocol = "") -> list[dict]:
        """
        Args:
            chain (str): _description_
            protocol (str)

        Returns:
            list[dict]:

        """
        # set return match vars
        _match = {"id": f"{chain}_{protocol}"}

        # return query
        return [{"$match": _match}, {"$unset": ["_id", "id"]}]

    @staticmethod
    def query_last(chain: Chain, protocol: Protocol) -> list[dict]:
        # set return match vars
        _match = {"chain": chain, "protocol": protocol}

        return [
            {"$match": _match},
            {"$sort": {"datetime": -1}},
            {"$limit": 3},
            {"$unset": ["_id", "id", "chain", "protocol"]},
        ]

    @staticmethod
    def query_hype_rewards(
        chain: Chain,
        hypervisor_address: str,
        ini_date: datetime = None,
        end_date: datetime = None,
    ) -> list[dict]:
        """Get hypervisor's rewards2
            sorted by datetime newest first

        Args:
            chain (Chain):
            hypervisor_address (str):
            ini_date (datetime, optional): . Defaults to None.
            end_date (datetime, optional): . Defaults to None.

        Returns:
            list[str]:
        """
        _match = {"chain": chain}
        if ini_date and end_date:
            _match["$and"] = [
                {"datetime": {"$gte": ini_date}},
                {"datetime": {"$lte": end_date}},
            ]
        elif ini_date:
            _match["datetime"] = {"$gte": ini_date}
        elif end_date:
            _match["datetime"] = {"$lte": end_date}

        return [
            {"$match": _match},
            {"$sort": {"datetime": -1}},
            {"$addFields": {"obj_as_arr": {"$objectToArray": "$$ROOT"}}},
            {"$unwind": "$obj_as_arr"},
            {"$match": {f"obj_as_arr.v.pools.{hypervisor_address}": {"$exists": 1}}},
            {
                "$project": {
                    "_id": 0,
                    "chain": "$chain",
                    "datetime": "$datetime",
                    "protocol": "$protocol",
                    "rewards2": f"$obj_as_arr.v.pools.{hypervisor_address}",
                }
            },
        ]


class db_aggregateStats_manager(db_collection_manager):
    def __init__(self, mongo_url: str):
        # Create a dictionary of collections
        self.db_collections = {"agregateStats": {"id": True}}
        # Set the database name
        self.db_name = "gamma_db_v1"

        super().__init__(
            mongo_url=mongo_url,
            db_name=self.db_name,
            db_collections=self.db_collections,
        )

        # Set the collection, which is the name of the collection in the database
        self.db_collection_name = "agregateStats"

    async def create_data(self, chain: Chain, protocol: Protocol) -> dict:
        """

        Args:
            chain (str): _description_
            protocol (str): _description_

        Returns:
            dict:
        """

        top_level = TopLevelData(protocol=protocol, chain=chain)
        top_level_data = await top_level.all_stats()

        dtime = datetime.now(timezone.utc)
        return {
            "id": f"{chain}_{protocol}_{dtime.timestamp()}",
            "chain": chain,
            "protocol": protocol,
            "datetime": dtime,
            "totalValueLockedUSD": top_level_data["tvl"],
            "pairCount": top_level_data["hypervisor_count"],
            "totalFeesClaimedUSD": top_level_data["fees_claimed"],
        }

    async def feed_db(self, chain: Chain, protocol: Protocol):
        try:
            # save as 1 item ( not separated)
            await self.save_item_to_database(
                data=await self.create_data(chain=chain, protocol=protocol),
                collection_name=self.db_collection_name,
            )
        except Exception:
            logger.warning(
                f" Unexpected error feeding  {chain}'s {protocol} aggregateStats to db   err:{sys.exc_info()[0]}"
            )

    async def get_data(self, chain: Chain, protocol: Protocol) -> dict:
        result = await self._get_data(
            query=self.query_last(chain=chain, protocol=protocol)
        )
        try:
            return result[0]
        except Exception:
            return {}

    @staticmethod
    def query_last(chain: Chain, protocol: Protocol = "") -> list[dict]:
        """Query last item ( highest datetime )
        Args:
            chain (str):
            protocol (str)

        Returns:
            list[dict]:

        """
        # set return match vars
        _match = {"chain": chain, "protocol": protocol}

        # return query
        return [
            {"$match": _match},
            {"$sort": {"datetime": -1}},
            {"$unset": ["_id", "id", "chain", "protocol"]},
        ]
