from enum import Enum

# import general enums
from sources.common.general.enums import Chain, Protocol


class PositionType(str, Enum):
    BASE = "base"
    LIMIT = "limit"


class QueryType(str, Enum):
    DATABASE = "database"
    SUBGRAPH = "subgraph"


class YieldType(str, Enum):
    TOTAL = "total"
    LP = "lp"
