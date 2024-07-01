from collections import defaultdict

from sources.common.general.config import get_config
from sources.subgraph.bins.enums import Chain, Protocol, QueryType

DEPLOYMENTS = [
    (Protocol.UNISWAP, Chain.ETHEREUM),
    (Protocol.UNISWAP, Chain.ARBITRUM),
    (Protocol.UNISWAP, Chain.OPTIMISM),
    (Protocol.UNISWAP, Chain.POLYGON),
    (Protocol.UNISWAP, Chain.BSC),
    (Protocol.UNISWAP, Chain.CELO),
    (Protocol.UNISWAP, Chain.MOONBEAM),
    (Protocol.UNISWAP, Chain.AVALANCHE),
    (Protocol.UNISWAP, Chain.BASE),
    (Protocol.UNISWAP, Chain.BLAST),
    (Protocol.UNISWAP, Chain.SCROLL),
    (Protocol.UNISWAP, Chain.LINEA),
    (Protocol.UNISWAP, Chain.MANTLE),
    (Protocol.UNISWAP, Chain.POLYGON_ZKEVM),
    (Protocol.UNISWAP, Chain.MANTA),
    (Protocol.UNISWAP, Chain.ROOTSTOCK),
    (Protocol.UNISWAP, Chain.TAIKO),
    (Protocol.QUICKSWAP, Chain.POLYGON),
    (Protocol.QUICKSWAP, Chain.POLYGON_ZKEVM),
    (Protocol.QUICKSWAP, Chain.ASTAR_ZKEVM),
    (Protocol.QUICKSWAP, Chain.IMMUTABLE_ZKEVM),
    (Protocol.QUICKSWAP, Chain.MANTA),
    (Protocol.QUICKSWAP, Chain.XLAYER),
    (Protocol.QUICKSWAP_UNISWAP, Chain.POLYGON_ZKEVM),
    (Protocol.CAMELOT, Chain.ARBITRUM),
    (Protocol.GLACIER, Chain.AVALANCHE),
    (Protocol.RETRO, Chain.POLYGON),
    (Protocol.STELLASWAP, Chain.MOONBEAM),
    (Protocol.BEAMSWAP, Chain.MOONBEAM),
    (Protocol.SPIRITSWAP, Chain.FANTOM),
    (Protocol.SUSHI, Chain.POLYGON),
    (Protocol.SUSHI, Chain.ARBITRUM),
    (Protocol.SUSHI, Chain.BASE),
    (Protocol.SUSHI, Chain.ROOTSTOCK),
    (Protocol.RAMSES, Chain.ARBITRUM),
    (Protocol.NILE, Chain.LINEA),
    (Protocol.ASCENT, Chain.POLYGON),
    (Protocol.FUSIONX, Chain.MANTLE),
    (Protocol.SYNTHSWAP, Chain.BASE),
    (Protocol.LYNEX, Chain.LINEA),
    (Protocol.PEGASYS, Chain.ROLLUX),
    (Protocol.BASEX, Chain.BASE),
    (Protocol.PANCAKESWAP, Chain.ARBITRUM),
    (Protocol.APERTURE, Chain.MANTA),
    (Protocol.HERCULES, Chain.METIS),
    (Protocol.BASESWAP, Chain.BASE),
    (Protocol.SWAPBASED, Chain.BASE),
    (Protocol.PHARAOH, Chain.AVALANCHE),
    (Protocol.SWAPR, Chain.GNOSIS),
    (Protocol.THENA, Chain.BSC),
    (Protocol.THENA, Chain.OPBNB),
    (Protocol.THICK, Chain.BASE),
    (Protocol.CLEOPATRA, Chain.MANTLE),
    (Protocol.BLASTER, Chain.BLAST),
    (Protocol.THRUSTER, Chain.BLAST),
    (Protocol.ZYBERSWAP, Chain.ARBITRUM),
    (Protocol.FENIX, Chain.BLAST),
    (Protocol.XTRADE, Chain.XLAYER),
    (Protocol.KIM, Chain.MODE),
    (Protocol.LINEHUB, Chain.LINEA),
    (Protocol.KINETIX, Chain.BASE),
]

# Protocol-Chains not supported by the subgraph but web3 api
THIRD_PARTY_REWARDERS = [
    (Protocol.ZYBERSWAP, Chain.ARBITRUM),
    (Protocol.THENA, Chain.BSC),
]

THEGRAPH_INDEX_NODE_URL = "https://api.thegraph.com/index-node/graphql"
ETH_BLOCKS_SUBGRAPH_URL = (
    "https://api.thegraph.com/subgraphs/name/blocklytics/ethereum-blocks"
)
UNI_V2_SUBGRAPH_URL = "https://api.thegraph.com/subgraphs/name/ianlapham/uniswapv2"

SUBGRAPH_STUDIO_KEY = get_config("SUBGRAPH_STUDIO_KEY")
SUBGRAPH_STUDIO_USER_KEY = get_config("SUBGRAPH_STUDIO_USER_KEY")

# dex_subgraph_urls = defaultdict(dict)
dex_hypepool_subgraph_urls = defaultdict(dict)
gamma_subgraph_urls = defaultdict(dict)

for protocol, chain in DEPLOYMENTS:
    subgraph_prefix = f"{protocol.value.upper()}_{chain.value.upper()}"
    # dex_subgraph_urls[protocol][chain] = get_config(f"{subgraph_prefix}_SUBGRAPH")
    dex_hypepool_subgraph_urls[protocol][chain] = get_config(
        f"{subgraph_prefix}_HP_SUBGRAPH"
    )
    gamma_subgraph_urls[protocol][chain] = get_config(
        f"{subgraph_prefix}_GAMMA_SUBGRAPH"
    )


XGAMMA_SUBGRAPH_URL = "https://api.thegraph.com/subgraphs/name/l0c4t0r/xgamma"
RECOVERY_POOL_URL = (
    "https://api.thegraph.com/subgraphs/name/l0c4t0r/gamma-recovery-pool"
)

DEFAULT_TIMEZONE = get_config("TIMEZONE")

CHARTS_CACHE_TIMEOUT = int(get_config("CHARTS_CACHE_TIMEOUT"))
APY_CACHE_TIMEOUT = int(get_config("APY_CACHE_TIMEOUT"))
DASHBOARD_CACHE_TIMEOUT = int(get_config("DASHBOARD_CACHE_TIMEOUT"))
ALLDATA_CACHE_TIMEOUT = int(get_config("ALLDATA_CACHE_TIMEOUT"))
DB_CACHE_TIMEOUT = int(get_config("DB_CACHE_TIMEOUT"))  # database calls cache

EXCLUDED_HYPERVISORS = list(filter(None, get_config("EXCLUDED_HYPES").split(",")))

legacy_stats = {
    "visr_distributed": 987998.1542393989,
    "visr_distributed_usd": 1246656.7073805775,
    "estimated_visr_annual_distribution": 1237782.0442017058,
    "estimated_visr_annual_distribution_usd": 1197097.0895269862,
}

MONGO_DB_URL = get_config("MONGO_DB_URL")
MONGO_DB_TIMEOUTMS = int(get_config("MONGO_DB_TIMEOUTMS"))
MONGO_DB_COLLECTIONS = {
    "static": {"id": True},  # no historic
    "returns": {"id": True},  # historic
    "allData": {"id": True},  # id = <chain_protocol>       no historic
    "allRewards2": {"id": True},  # id = <chain_protocol>   no historic
    "agregateStats": {"id": True},  # id = <chain_protocol_timestamp>    historic
}

# local chain name <-> standard chain short name convention as in
# https://chainid.network/chains.json  or https://chainid.network/chains_mini.json
CHAIN_NAME_CONVERSION = {
    "eth": Chain.ETHEREUM,
    "matic": Chain.POLYGON,
    "oeth": Chain.OPTIMISM,
    "arb1": Chain.ARBITRUM,
    "celo": Chain.CELO,
}

# Max fees per rebalance to remove outliers
GROSS_FEES_MAX = 10**6
TVL_MAX = 100e6

GQL_CLIENT_TIMEOUT = int(get_config("GQL_CLIENT_TIMEOUT"))

# What to run first, subgraph or database
RUN_FIRST_QUERY_TYPE = QueryType(get_config("RUN_FIRST_QUERY_TYPE"))

MASTERCHEF_ADDRESSES = get_config("MASTERCHEF_ADDRESSES")
