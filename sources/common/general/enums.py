from enum import Enum


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
    ASTAR_ZKEVM = (
        "astar_zkevm",
        3776,
        "astar-zkevm",
        "Astar zkEVM",
        None,
        None,
        "Astar zkEVM",
    )
    IMMUTABLE_ZKEVM = (
        "immutable_zkevm",
        13371,
        "immutable-zkevm",
        "Immutable zkEVM",
        None,
        None,
        "Immutable zkEVM",
    )
    AVALANCHE = ("avalanche", 43114, "avalanche", "Avalanche", None, None, "Avalanche")
    FANTOM = ("fantom", 250, "fantom", "Fantom", None, None, "Fantom")
    MOONBEAM = ("moonbeam", 1284, "moonbeam", "Moonbeam", None, None, "Moonbeam")
    MANTLE = ("mantle", 5000, "mantle", "Mantle", None, None, "Mantle")
    BASE = ("base", 8453, "base", "Base", None, None, "Base")
    LINEA = ("linea", 59144, "linea", "Linea", None, None, "Linea")
    ROLLUX = ("rollux", 570, "rollux", "Rollux", None, None, "Rollux")
    MANTA = ("manta", 169, "manta", "Manta", None, None, "Manta")
    METIS = ("metis", 1088, "metis", "Metis", None, None, "Metis")
    OPBNB = ("opbnb", 204, "opbnb", "Opbnb", None, None, "Opbnb")
    GNOSIS = ("gnosis", 100, "gnosis", "Gnosis", None, None, "Gnosis")
    BLAST = ("blast", 81457, "blast", "Blast", None, None, "Blast")
    SCROLL = ("scroll", 534352, "scroll", "Scroll", None, None, "Scroll")
    XLAYER = ("xlayer", 196, "xlayer", "XLayer", None, None, "XLayer")
    MODE = ("mode", 34443, "mode", "Mode", None, None, "Mode")
    ROOTSTOCK = ("rootstock", 30, "rootstock", "Rootstock", None, None, "Rootstock")
    TAIKO = ("taiko", 167000, "taiko", "Taiko", None, None, "Taiko")
    SEI = ("sei", 1329, "sei", "Sei", None, None, "Sei")
    IOTA = ("iota", 8822, "iota", "Iota", None, "iota_evm", "Iota")
    ZIRCUIT = ("zircuit", 48900, "zircuit", "Zircuit", None, None, "Zircuit")
    CORE = ("core", 1116, "core", "Core", None, None, "Core")
    BARTIO = (
        "berachain_bartio",
        80084,
        "berachain-bartio",
        "Berachain bArtio",
        None,
        None,
        "Berachain bArtio",
    )
    WORLDCHAIN = ("worldchain", 480, "worldchain", "Worldchain", None, None, "Worldchain")

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
    QUICKSWAP_UNISWAP = "quickswap_uniswap"
    UNISWAP = "uniswapv3"
    ZYBERSWAP = "zyberswap"
    THENA = "thena"
    GLACIER = "glacier"
    CAMELOT = "camelot"
    RETRO = "retro"
    STELLASWAP = "stellaswap"
    SUSHI = "sushi"
    RAMSES = "ramses"
    NILE = "nile"
    BEAMSWAP = "beamswap"
    ASCENT = "ascent"
    FUSIONX = "fusionx"
    SYNTHSWAP = "synthswap"
    LYNEX = "lynex"
    PEGASYS = "pegasys"
    BASEX = "basex"
    PANCAKESWAP = "pancakeswap"
    APERTURE = "aperture"
    HERCULES = "hercules"
    BASESWAP = "baseswap"
    SWAPBASED = "swapbased"
    PHARAOH = "pharaoh"
    SWAPR = "swapr"
    THICK = "thick"
    CLEOPATRA = "cleopatra"
    BLASTER = "blaster"
    THRUSTER = "thruster"
    FENIX: "fenix"
    XTRADE: "xtrade"
    KIM: "kim"
    LINEHUB: "linehub"
    KINETIX: "kinetix"
    WAGMI: "wagmi"
    SCRIBE: "scribe"
    CIRCUIT: "circuit"
    GMEOW: "gmeow"
    CORE: "core"
    KODIAK: "kodiak"


class Protocol(str, Enum):
    #  ( value , api_url, api_name, subgraph_name, database_name, fantasy_name )
    GAMMA = ("gamma", None, None, None, "gamma", "Gamma Strategies")

    ALGEBRAv3 = ("algebrav3", None, None, None, "algebrav3", "Algebra V3")
    UNISWAPv3 = ("uniswapv3", None, None, None, "uniswapv3", "Uniswap V3")

    QUICKSWAP = ("quickswap", None, None, None, "quickswap", "QuickSwap")
    QUICKSWAP_UNISWAP = (
        "quickswap_uniswap",
        "quickswap-uniswap",
        None,
        None,
        "quickswap-uniswap",
        "QuickSwap-Uniswap",
    )
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
    NILE = ("nile", None, None, None, "nile", "Nile")
    VEZARD = ("vezard", None, None, None, "vezard", "veZard")
    SUSHI = ("sushi", None, None, None, "sushi", "Sushi")
    ASCENT = ("ascent", None, None, None, "ascent", "Ascent")
    FUSIONX = ("fusionx", None, None, None, "fusionx", "Fusionx")
    SYNTHSWAP = ("synthswap", None, None, None, "synthswap", "Synthswap")
    LYNEX = ("lynex", None, None, None, "lynex", "Lynex")
    PEGASYS = ("pegasys", None, None, None, "pegasys", "Pegasys")
    BASEX = ("basex", None, None, None, "basex", "BaseX")
    PANCAKESWAP = ("pancakeswap", None, None, None, "pancakeswap", "Pancakeswap")
    APERTURE = ("aperture", None, None, None, "aperture", "Aperture")
    HERCULES = ("hercules", None, None, None, "hercules", "Hercules")
    BASESWAP = ("baseswap", None, None, None, "baseswap", "Baseswap")
    SWAPBASED = ("swapbased", None, None, None, "swapbased", "Swapbased")
    PHARAOH = ("pharaoh", None, None, None, "pharaoh", "Pharaoh")
    SWAPR = ("swapr", None, None, None, "swapr", "Swapr")
    THICK = ("thick", None, None, None, "thick", "Thick")
    CLEOPATRA = ("cleopatra", None, None, None, "cleopatra", "Cleopatra")
    BLASTER = ("blaster", None, None, None, "blaster", "Blaster")
    THRUSTER = ("thruster", None, None, None, "thruster", "Thruster")
    FENIX = ("fenix", None, None, None, "fenix", "Fenix")
    XTRADE = ("xtrade", None, None, None, "xtrade", "Xtrade")
    KIM = ("kim", None, None, None, "kim", "Kim")
    LINEHUB = ("linehub", None, None, None, "linehub", "Linehub")
    KINETIX = ("kinetix", None, None, None, "kinetix", "Kinetix")
    WAGMI = ("wagmi", None, None, None, "wagmi", "Wagmi")
    SCRIBE = ("scribe", None, None, None, "scribe", "Scribe")
    GMEOW = ("gmeow", None, None, None, "gmeow", "Gmeow")
    CIRCUIT = ("circuit", None, None, None, "circuit", "Circuit")
    GLYPH = ("glyph", None, None, None, "glyph", "Glyph")
    COREX = ("corex", None, None, None, "corex", "Corex")
    VELODROME = ("velodrome", None, None, None, "velodrome", "Velodrome")
    AERODROME = ("aerodrome", None, None, None, "aerodrome", "Aerodrome")
    KODIAK = ("kodiak", None, None, None, "kodiak", "Kodiak")

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

    ANGLE_MERKLE = "angle_merkle"

    RAMSES_v2 = "ramses_v2"


# HELPERS
def text_to_chain(text: str) -> Chain:
    """Text to Chain conversion

    Args:
        text (str): what to find

    Returns:
        Chain:
    """
    for chain in Chain:
        if text.lower() in [
            chain.value.lower(),
            chain.database_name.lower(),
            chain.fantasy_name.lower(),
        ]:
            return chain

    if text.lower() == "polygon-zkevm":
        return Chain.POLYGON_ZKEVM
    elif text.lower() == "mainnet":
        return Chain.ETHEREUM
    raise ValueError(f"Chain with text {text} not found")


def int_to_chain(num: int) -> Chain:
    """Chain id to Chain enum conversion

    Args:
        num (int): chain id

    Returns:
        Protocol:
    """
    for ch in Chain:
        if num == ch.id:
            return ch
    raise ValueError(f"Chain id {num} not found")


def text_to_protocol(text: str) -> Protocol:
    """Text to Protocol conversion

    Args:
        text (str): what to find

    Returns:
        Protocol:
    """
    for protocol in Protocol:
        if text.lower() in [
            protocol.value.lower(),
            protocol.database_name.lower(),
            protocol.fantasy_name.lower(),
        ]:
            return protocol
    raise ValueError(f"Protocol with text {text} not found")


def int_to_period(num: int) -> Period:
    """Day numbers to Protocol conversion

    Args:
        num (int): days

    Returns:
        Protocol:
    """
    for per in Period:
        if num == per.days:
            return per
    raise ValueError(f"Period with days {num} not found")
