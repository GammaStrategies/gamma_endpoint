from enum import Enum, unique


class Chain(str, Enum):
    ARBITRUM = "arbitrum"
    CELO = "celo"
    ETHEREUM = "ethereum"
    OPTIMISM = "optimism"
    POLYGON = "polygon"
    BSC = "bsc"


class Dex(str, Enum):
    QUICKSWAP = "quickswap"
    UNISWAP = "uniswap"
    ZYBERSWAP = "zyberswap"
    THENA = "thena"


@unique
class ChainId(int, Enum):
    ARBITRUM = 42161
    CELO = 42220
    ETHEREUM = 1
    OPTIMISM = 10
    POLYGON = 137
    BSC = 56
