from decimal import Decimal, localcontext
import logging
import asyncio
from math import log

from bson.decimal128 import Decimal128, create_decimal128_context
from pymongo.errors import BulkWriteError
from sources.common.database.common.db_managers import MongoDbManager

logger = logging.getLogger(__name__)


class db_collections_common:
    def __init__(
        self,
        mongo_url: str,
        db_name: str,
        db_collections: dict,
    ):
        self._db_mongo_url = mongo_url
        self._db_name = db_name
        self._db_collections = db_collections

    # actual db saving
    async def save_items_to_database(
        self,
        data: dict,
        collection_name: str,
    ):
        """Save dictionary values to the database collection replacing any equal id defined

        Args:
            data (list): data list following tool_mongodb_general class to be saved to database in a dict format
            collection_name (str): collection name to save data to
        """
        # add item by item to database
        requests = [
            self.save_item_to_database(data=item, collection_name=collection_name)
            for key, item in data.items()
        ]

        await asyncio.gather(*requests)

    async def save_item_to_database(
        self,
        data: dict,
        collection_name: str,
    ):
        """Save dictionary values to the database collection replacing any equal id defined

        Args:
            data (list): data list following tool_mongodb_general class to be saved to database in a dict format
            collection_name (str): collection name to save data to
        """
        try:
            with MongoDbManager(
                url=self._db_mongo_url,
                db_name=self._db_name,
                collections=self._db_collections,
            ) as _db_manager:
                # add to mongodb
                _db_manager.add_item(
                    coll_name=collection_name, dbFilter={"id": data["id"]}, data=data
                )
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Unable to save data to mongo's {collection_name} collection.  error-> {e}"
            )

    async def replace_item_to_database(
        self,
        data: dict,
        collection_name: str,
    ):
        try:
            with MongoDbManager(
                url=self._db_mongo_url,
                db_name=self._db_name,
                collections=self._db_collections,
            ) as _db_manager:
                # add to mongodb
                _db_manager.replace_item(
                    coll_name=collection_name, dbFilter={"id": data["id"]}, data=data
                )
        except Exception as e:
            logging.getLogger(__name__).exception(
                f" Unable to replace data in mongo's {collection_name} collection.  error-> {e}"
            )

    async def replace_items_to_database(
        self,
        data: list[dict],
        collection_name: str,
    ):
        """Replace multiple items in a collection at once ( in bulk)

        Args:
            data (list[dict]): _description_
            collection_name (str): _description_
        """
        try:
            # create bulk data object
            bulk_data = [{"filter": {"id": item["id"]}, "data": item} for item in data]

            with MongoDbManager(
                url=self._db_mongo_url,
                db_name=self._db_name,
                collections=self._db_collections,
            ) as _db_manager:
                # add to mongodb
                _db_manager.replace_items_bulk(
                    coll_name=collection_name, data=bulk_data, upsert=True
                )
        except BulkWriteError as bwe:
            logging.getLogger(__name__).error(
                f"  Error while replacing multiple items in {collection_name} collection database. Items qtty: {len(data)}  error-> {bwe.details}"
            )
        except Exception as e:
            logging.getLogger(__name__).error(
                f" Unable to replace multiple items in mongo's {collection_name} collection.  Items qtty: {len(data)}    error-> {e}"
            )

    async def query_items_from_database(
        self,
        query: list[dict],
        collection_name: str,
    ) -> list:
        # db_manager = self.create_db_manager()
        with MongoDbManager(
            url=self._db_mongo_url,
            db_name=self._db_name,
            collections=self._db_collections,
        ) as _db_manager:
            result = list(
                _db_manager.get_items(coll_name=collection_name, aggregate=query)
            )
        return result

    async def get_items_from_database(self, collection_name: str, **kwargs) -> list:
        with MongoDbManager(
            url=self._db_mongo_url,
            db_name=self._db_name,
            collections=self._db_collections,
        ) as _db_manager:
            result = _db_manager.get_items(coll_name=collection_name, **kwargs)
            result = list(result)
        return result

    async def get_distinct_items_from_database(
        self, field: str, collection_name: str, condition: dict = None
    ) -> list:
        with MongoDbManager(
            url=self._db_mongo_url,
            db_name=self._db_name,
            collections=self._db_collections,
        ) as _db_manager:
            result = list(
                _db_manager.get_distinct(
                    coll_name=collection_name, field=field, condition=condition or {}
                )
            )

        return result

    # TOOLING

    @staticmethod
    def convert_decimal_to_d128(item: dict) -> dict:
        """Converts a dictionary decimal values to BSON.decimal128, recursivelly.
            The function iterates a dict looking for types of Decimal and converts them to Decimal128.
            Embedded dictionaries and lists are called recursively.

        Args:
            item (dict):

        Returns:
            dict: converted values dict
        """
        if item is None:
            return None

        for k, v in list(item.items()):
            if isinstance(v, dict):
                db_collections_common.convert_decimal_to_d128(v)
            elif isinstance(v, list):
                for l in v:
                    if isinstance(l, dict):
                        db_collections_common.convert_decimal_to_d128(l)
            elif isinstance(v, Decimal):
                decimal128_ctx = create_decimal128_context()
                with localcontext(decimal128_ctx) as ctx:
                    item[k] = Decimal128(ctx.create_decimal(str(v)))

        return item

    @staticmethod
    def convert_d128_to_decimal(item: dict) -> dict:
        """Converts a dictionary decimal128 values to decimal, recursivelly.
            The function iterates a dict looking for types of Decimal128 and converts them to Decimal.
            Embedded dictionaries and lists are called recursively.

        Args:
            item (dict):

        Returns:
            dict: converted values dict
        """
        if item is None:
            return None

        for k, v in list(item.items()):
            if isinstance(v, dict):
                db_collections_common.convert_d128_to_decimal(v)
            elif isinstance(v, list):
                for l in v:
                    if isinstance(l, dict):
                        db_collections_common.convert_d128_to_decimal(l)
            elif isinstance(v, Decimal128):
                item[k] = v.to_decimal()

        return item

    @staticmethod
    def convert_decimal_to_float(item: dict) -> dict:
        """Converts a dictionary decimal values to float, recursivelly.
            The function iterates a dict looking for types of Decimal and converts them to float.
            Embedded dictionaries and lists are called recursively.

        Args:
            item (dict):

        Returns:
            dict: converted values dict
        """
        if item is None:
            return None

        for k, v in list(item.items()):
            if isinstance(v, dict):
                db_collections_common.convert_decimal_to_float(v)
            elif isinstance(v, list):
                for l in v:
                    if isinstance(l, dict):
                        db_collections_common.convert_decimal_to_float(l)
            elif isinstance(v, Decimal):
                item[k] = float(v)

        return item
