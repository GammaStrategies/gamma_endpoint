from enum import Enum, unique


class Chain(str, Enum):
    #            ( value , id , url, fantasy_name )
    ARBITRUM = ("arbitrum", 42161, "arbitrum", "Arbitrum")
    CELO = ("celo", 42220, "celo", "Celo")
    ETHEREUM = ("ethereum", 1, "ethereum", "Ethereum")
    OPTIMISM = ("optimism", 10, "optimism", "Optimism")
    POLYGON = ("polygon", 137, "polygon", "Polygon")
    BSC = ("bsc", 56, "bsc", "Binance Chain")
    POLYGON_ZKEVM = ("polygon_zkevm", 1101, "polygon-zkevm", "Polygon zkevm")
    AVALANCHE = ("avalanche", 43114, "avalanche", "Avalanche")
    FANTOM = ("fantom", 250, "fantom", "Fantom")
    MOONBEAM = ("moonbeam", 1287, "moonbeam", "Moonbeam")

    # extra properties
    id: int
    url: str
    fantasy_name: str

    def __new__(self, value: str, id: int, url: str, fantasy_name: str):
        """

        Args:
            value (_type_): chain name
            id (_type_): chain id

        Returns:
            : Chain
        """
        obj = str.__new__(self)
        obj._value_ = value
        obj.id = id
        obj.url = url
        obj.fantasy_name = fantasy_name
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
