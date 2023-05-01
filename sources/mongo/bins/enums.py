from enum import Enum
from sources.common.general import enums as general_enums


class Chain(str, Enum):
    ARBITRUM = "arbitrum"
    CELO = "celo"
    ETHEREUM = "ethereum"
    OPTIMISM = "optimism"
    POLYGON = "polygon"
    BSC = "binance"


class Dex(str, Enum):
    QUICKSWAP = "quickswap"
    UNISWAP = "uniswapv3"
    ZYBERSWAP = "zyberswap"
    THENA = "thena"


# converters
class enumsConverter:
    @staticmethod
    def convert_local_to_general(
        chain: Chain | None = None, dex: Dex | None = None
    ) -> general_enums.Chain | general_enums.Dex:
        if chain:
            # if chain == Chain.MAINNET:
            #     return general_enums.Chain.ETHEREUM
            # else:
            return getattr(general_enums.Chain, chain.name)
        elif dex:
            return getattr(general_enums.Dex, dex.name)

    @staticmethod
    def convert_general_to_local(
        chain: general_enums.Chain | None = None, dex: general_enums.Dex | None = None
    ) -> Chain | Dex:
        if chain:
            return getattr(Chain, chain.name)
        elif dex:
            return getattr(Dex, dex.name)
