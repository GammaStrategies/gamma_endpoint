from enum import Enum, unique


class Chain(str, Enum):
    #       ( value , id , API url, API name, subgraph name, database name, fantasy_name )
    ARBITRUM = ("arbitrum", 42161, "arbitrum", "Arbitrum", None, None, "Arbitrum")
    CELO = ("celo", 42220, "celo", "Celo", None, None, "Celo")
    ETHEREUM = ("mainnet", 1, "mainnet", "Ethereum", None, None, "Ethereum")
    OPTIMISM = ("optimism", 10, "optimism", "Optimism", None, None, "Optimism")
    POLYGON = ("polygon", 137, "polygon", "Polygon", None, None, "Polygon")
    BSC = ("bsc", 56, "bsc", "Binance chain", "bsc", "binance", "Binance Chain")
    POLYGON_ZKEVM = (
        "polygon_zkevm",
        1101,
        "polygon-zkevm",
        "Polygon zkEVM",
        None,
        None,
        "Polygon zkEVM",
    )
    AVALANCHE = ("avalanche", 43114, "avalanche", "Avalanche", None, None, "Avalanche")
    FANTOM = ("fantom", 250, "fantom", "Fantom", None, None, "Fantom")
    MOONBEAM = ("moonbeam", 1287, "moonbeam", "Moonbeam", None, None, "Moonbeam")

    # extra properties
    id: int
    api_url: str
    api_name: str
    subgraph_name: str
    database_name: str
    fantasy_name: str

    def __new__(
        self,
        value: str,
        id: int,
        api_url: str | None = None,
        api_name: str | None = None,
        subgraph_name: str | None = None,
        database_name: str | None = None,
        fantasy_name: str | None = None,
    ):
        """_summary_

        Args:
            value (str): _description_
            id (int): _description_
            api_url (str | None, optional): . Defaults to value.
            api_name (str | None, optional): . Defaults to value.
            subgraph_name (str | None, optional): . Defaults to value.
            database_name (str | None, optional): . Defaults to value.
            fantasy_name (str | None, optional): . Defaults to value.

        Returns:
            _type_: _description_
        """
        obj = str.__new__(self, value)
        obj._value_ = value
        obj.id = id
        # optional properties
        obj.api_url = api_url or value.lower()
        obj.api_name = api_name or value.lower()
        obj.subgraph_name = subgraph_name or value.lower()
        obj.database_name = database_name or value.lower()
        obj.fantasy_name = fantasy_name or value.lower()
        return obj


class Dex(str, Enum):
    QUICKSWAP = "quickswap"
    UNISWAP = "uniswap"
    ZYBERSWAP = "zyberswap"
    THENA = "thena"
    GLACIER = "glacier"
    CAMELOT = "camelot"
    RETRO = "retro"
    STELLASWAP = "stellaswap"


@unique
class ChainId(int, Enum):
    ARBITRUM = 42161
    CELO = 42220
    ETHEREUM = 1
    OPTIMISM = 10
    POLYGON = 137
    BSC = 56
    POLYGON_ZKEVM = 1101
    AVALANCHE = 43114


class Protocol(str, Enum):
    #            ( value , url, fantasy_name )
    GAMMA = ("gamma", None, None, None, "Gamma Strategies")

    ALGEBRAv3 = ("algebrav3", None, None, None, "UniswapV3")
    UNISWAPv3 = ("uniswapv3", None, None, None, "AlgebraV3")

    QUICKSWAP = ("quickswap", None, None, None, "QuickSwap")
    UNISWAP = ("uniswap", None, None, None, "Uniswap")
    ZYBERSWAP = ("zyberswap", None, None, None, "Zyberswap")
    THENA = ("thena", None, None, None, "Thena")
    GLACIER = ("glacier", None, None, None, "Glacier")
    SPIRITSWAP = ("spiritswap", None, None, None, "SpiritSwap")
    CAMELOT = ("camelot", None, None, None, "Camelot")
    RETRO = ("retro", None, None, None, "Retro")
    STELLASWAP = ("stellaswap", None, None, None, "Stellaswap")
    BEAMSWAP = ("beamswap", None, None, None, "Beamswap")
    RAMSES = ("ramses", None, None, None, "Ramses")
    VEZARD = ("vezard", None, None, None, "veZard")

    # extra properties
    api_url: str
    api_name: str
    subgraph_name: str
    database_name: str
    fantasy_name: str

    def __new__(
        self,
        value: str,
        api_url: str | None = None,
        api_name: str | None = None,
        subgraph_name: str | None = None,
        database_name: str | None = None,
        fantasy_name: str | None = None,
    ):
        """

        Args:
            value (_type_): chain name
            id (_type_): chain id

        Returns:
            : Chain
        """
        obj = str.__new__(self, value)
        obj._value_ = value
        # optional properties
        obj.api_url = api_url or value.lower()
        obj.api_name = api_name or value.lower()
        obj.subgraph_name = subgraph_name or value.lower()
        obj.database_name = database_name or value.lower()
        obj.fantasy_name = fantasy_name or value.lower()
        return obj


class Period(str, Enum):
    ##       value  api_url, api_name, subgraph_name, database_name, cron, days
    DAILY = (
        "daily",
        None,
        None,
        None,
        None,
        "*/60 */2 * * *",
        1,
    )  # (At every 60th minute past every 2nd hour. )
    WEEKLY = (
        "weekly",
        None,
        None,
        None,
        None,
        "*/60 */12 * * *",
        7,
    )  # (At every 60th minute past every 12th hour. )
    BIWEEKLY = (
        "biweekly",
        None,
        None,
        None,
        None,
        "0 6 */1 * *",
        14,
    )  # ( At 06:00 on every day-of-month.)
    MONTHLY = (
        "monthly",
        None,
        None,
        None,
        None,
        "0 12 */2 * *",
        30,
    )  # ( At 12:00 on every 2nd day-of-month.)
    # BIMONTHLY = ("bimonthly", None, None, None, None, None, "0 12 */4 * *", 60)
    TRIMONTHLY = (
        "trimonthly",
        None,
        None,
        None,
        None,
        "0 4 */6 * *",
        90,
    )  # ( At 00:00 on every 6th day-of-month.)
    SEMESTRIAL = (
        "semestrial",
        None,
        None,
        None,
        None,
        "0 15 */12 * *",
        180,
    )  # ( At 00:00 on every 12th day-of-month.)
    YEARLY = ("yearly", None, None, None, None, "0 2 */24 * *", 365)

    # extra properties
    api_url: str
    api_name: str
    subgraph_name: str
    database_name: str
    cron: str
    days: int

    def __new__(
        self,
        value: str,
        api_url: str | None = None,
        api_name: str | None = None,
        subgraph_name: str | None = None,
        database_name: str | None = None,
        cron: str | None = None,
        days: int | None = None,
    ):
        """

        Args:
            value (_type_): chain name
            id (_type_): chain id

        Returns:
            : Chain
        """
        obj = str.__new__(self, value)
        obj._value_ = value
        # optional properties
        obj.api_url = api_url or value.lower()
        obj.api_name = api_name or value.lower()
        obj.subgraph_name = subgraph_name or value.lower()
        obj.database_name = database_name or value.lower()
        obj.cron = cron or ""
        obj.days = days or 0
        return obj
