from enum import Enum


class Chain(str, Enum):
    ARBITRUM = "arbitrum"
    CELO = "celo"
    MAINNET = "mainnet"
    OPTIMISM = "optimism"
    POLYGON = "polygon"
    BSC = "bsc"
    POLYGON_ZKEVM = "polygon_zkevm"
    AVALANCHE = "avalanche"
    FANTOM = "fantom"
    MOONBEAM = "moonbeam"


class PositionType(str, Enum):
    BASE = "base"
    LIMIT = "limit"


class Protocol(str, Enum):
    QUICKSWAP = "quickswap"
    UNISWAP = "uniswap"
    ZYBERSWAP = "zyberswap"
    THENA = "thena"
    CAMELOT = "camelot"
    GLACIER = "glacier"
    RETRO = "retro"


class QueryType(str, Enum):
    DATABASE = "database"
    SUBGRAPH = "subgraph"


class YieldType(str, Enum):
    TOTAL = "total"
    LP = "lp"


from sources.common.general import enums as general_enums


# converters
class enumsConverter:
    @staticmethod
    def convert_local_to_general(
        chain: Chain | None = None, protocol: Protocol | None = None
    ) -> general_enums.Chain | general_enums.Dex:
        if chain:
            if chain == Chain.MAINNET:
                return general_enums.Chain.ETHEREUM
            else:
                return getattr(general_enums.Chain, chain.name)
        elif protocol:
            return getattr(general_enums.Dex, protocol.name)

    @staticmethod
    def convert_general_to_local(
        chain: general_enums.Chain | None = None, dex: general_enums.Dex | None = None
    ) -> Chain | Protocol:
        if chain:
            if chain == general_enums.Chain.ETHEREUM:
                return Chain.MAINNET
            else:
                return getattr(Chain, chain.name)
        elif dex:
            return getattr(Protocol, dex.name)
