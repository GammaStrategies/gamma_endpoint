from enum import Enum, unique


class Chain(str, Enum):
    #       ( value , id , API url, API name, subgraph name, database name, fantasy_name )
    ARBITRUM = ("arbitrum", 42161, "arbitrum", "Arbitrum", None, None, "Arbitrum")
    CELO = ("celo", 42220, "celo", "Celo", None, None, "Celo")
    ETHEREUM = ("mainnet", 1, "mainnet", "Ethereum", None, "ethereum", "Ethereum")
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
    UNISWAP = "uniswapv3"
    ZYBERSWAP = "zyberswap"
    THENA = "thena"
    GLACIER = "glacier"
    CAMELOT = "camelot"
    RETRO = "retro"
    STELLASWAP = "stellaswap"
    SUSHI = "sushi"


class Protocol(str, Enum):
    #  ( value , api_url, api_name, subgraph_name, database_name, fantasy_name )
    GAMMA = ("gamma", None, None, None, "gamma", "Gamma Strategies")

    ALGEBRAv3 = ("algebrav3", None, None, None, "algebrav3", "Algebra V3")
    UNISWAPv3 = ("uniswapv3", None, None, None, "uniswapv3", "Uniswap V3")

    QUICKSWAP = ("quickswap", None, None, None, "quickswap", "QuickSwap")
    UNISWAP = ("uniswap", None, None, None, "uniswapv3", "Uniswap")
    ZYBERSWAP = ("zyberswap", None, None, None, "zyberswap", "Zyberswap")
    THENA = ("thena", None, None, None, "thena", "Thena")
    GLACIER = ("glacier", None, None, None, "glacier", "Glacier")
    SPIRITSWAP = ("spiritswap", None, None, None, "spiritswap", "SpiritSwap")
    CAMELOT = ("camelot", None, None, None, "camelot", "Camelot")
    RETRO = ("retro", None, None, None, "retro", "Retro")
    STELLASWAP = ("stellaswap", None, None, None, "stellaswap", "Stellaswap")
    BEAMSWAP = ("beamswap", None, None, None, "beamswap", "Beamswap")
    RAMSES = ("ramses", None, None, None, "ramses", "Ramses")
    VEZARD = ("vezard", None, None, None, "vezard", "veZard")
    SUSHI = ("sushi", None, None, None, "sushi", "Sushi")

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
    QUARTERLY = (
        "quarterly",
        None,
        None,
        None,
        None,
        "0 4 */6 * *",
        90,
    )  # ( At 00:00 on every 6th day-of-month.)
    BIANNUAL = (
        "biannual",
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


class rewarderType(str, Enum):
    GAMMA_masterchef_v1 = "gamma_masterchef_v1"
    GAMMA_masterchef_v2 = "gamma_masterchef_v2"

    ZYBERSWAP_masterchef_v1 = "zyberswap_masterchef_v1"
    ZYBERSWAP_masterchef_v1_rewarder = "zyberswap_masterchef_v1_rewarder"
    THENA_gauge_v2 = "thena_gauge_v2"
    THENA_voter_v3 = "thena_voter_v3"
    BEAMSWAP_masterchef_v2 = "beamswap_masterchef_v2"
    BEAMSWAP_masterchef_v2_rewarder = "beamswap_masterchef_v2_rewarder"
