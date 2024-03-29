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
    (Protocol.QUICKSWAP, Chain.POLYGON),
    (Protocol.ZYBERSWAP, Chain.ARBITRUM),
    (Protocol.THENA, Chain.BSC),
    (Protocol.THENA, Chain.OPBNB),
    (Protocol.QUICKSWAP, Chain.POLYGON_ZKEVM),
    (Protocol.QUICKSWAP, Chain.ASTAR_ZKEVM),
    (Protocol.QUICKSWAP, Chain.IMMUTABLE_ZKEVM),
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
    (Protocol.RAMSES, Chain.ARBITRUM),
    (Protocol.ASCENT, Chain.POLYGON),
    (Protocol.FUSIONX, Chain.MANTLE),
    (Protocol.SYNTHSWAP, Chain.BASE),
    (Protocol.LYNEX, Chain.LINEA),
    (Protocol.PEGASYS, Chain.ROLLUX),
    (Protocol.BASEX, Chain.BASE),
    (Protocol.PANCAKESWAP, Chain.ARBITRUM),
    (Protocol.APERTURE, Chain.MANTA),
    (Protocol.QUICKSWAP, Chain.MANTA),
    (Protocol.HERCULES, Chain.METIS),
    (Protocol.BASESWAP, Chain.BASE),
    (Protocol.SWAPBASED, Chain.BASE),
    (Protocol.PHARAOH, Chain.AVALANCHE),
    (Protocol.SWAPR, Chain.GNOSIS),
    (Protocol.THICK, Chain.BASE),
    (Protocol.CLEOPATRA, Chain.MANTLE),
    (Protocol.BLASTER, Chain.BLAST),
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


DEX_SUBGRAPH_URLS = {
    Protocol.UNISWAP: {
        Chain.ETHEREUM: get_config("UNISWAP_MAINNET_SUBGRAPH_URL"),
        Chain.POLYGON: get_config("UNISWAP_POLYGON_SUBGRAPH_URL"),
        Chain.ARBITRUM: get_config("UNISWAP_ARBITRUM_SUBGRAPH_URL"),
        Chain.OPTIMISM: get_config("UNISWAP_OPTIMISM_SUBGRAPH_URL"),
        Chain.CELO: get_config("UNISWAP_CELO_SUBGRAPH_URL"),
        Chain.BSC: get_config("UNISWAP_BSC_SUBGRAPH_URL"),
        Chain.MOONBEAM: get_config("UNISWAP_MOONBEAM_SUBGRAPH_URL"),
        Chain.AVALANCHE: get_config("UNISWAP_AVALANCHE_SUBGRAPH_URL"),
    },
    Protocol.QUICKSWAP: {
        Chain.POLYGON: get_config("QUICKSWAP_POLYGON_SUBGRAPH_URL"),
        Chain.POLYGON_ZKEVM: get_config("QUICKSWAP_POLYGON_ZKEVM_SUBGRAPH_URL"),
        Chain.MANTA: get_config("QUICKSWAP_MANTA_SUBGRAPH_URL"),
        Chain.ASTAR_ZKEVM: get_config("QUICKSWAP_ASTAR_ZKEVM_SUBGRAPH_URL"),
        Chain.IMMUTABLE_ZKEVM: get_config("QUICKSWAP_IMMUTABLE_SUBGRAPH_URL")
    },
    Protocol.QUICKSWAP_UNISWAP: {
        Chain.POLYGON_ZKEVM: get_config("QUICKSWAP_UNISWAP_POLYGON_ZKEVM_SUBGRAPH_URL"),
    },
    Protocol.ZYBERSWAP: {
        Chain.ARBITRUM: get_config("ZYBERSWAP_ARBITRUM_SUBGRAPH_URL"),
    },
    Protocol.THENA: {
        Chain.BSC: get_config("THENA_BSC_SUBGRAPH_URL"),
        Chain.OPBNB: get_config("THENA_OPBNB_SUBGRAPH_URL"),
    },
    Protocol.CAMELOT: {
        Chain.ARBITRUM: get_config("CAMELOT_ARBITRUM_SUBGRAPH_URL"),
    },
    Protocol.GLACIER: {
        Chain.AVALANCHE: get_config("GLACIER_AVALANCHE_SUBGRAPH_URL"),
    },
    Protocol.RETRO: {
        Chain.POLYGON: get_config("RETRO_POLYGON_SUBGRAPH_URL"),
    },
    Protocol.STELLASWAP: {
        Chain.MOONBEAM: get_config("STELLASWAP_MOONBEAM_SUBGRAPH_URL"),
    },
    Protocol.BEAMSWAP: {
        Chain.MOONBEAM: get_config("BEAMSWAP_MOONBEAM_SUBGRAPH_URL"),
    },
    Protocol.SPIRITSWAP: {
        Chain.FANTOM: get_config("SPIRITSWAP_FANTOM_SUBGRAPH_URL"),
    },
    Protocol.SUSHI: {
        Chain.POLYGON: get_config("SUSHI_POLYGON_SUBGRAPH_URL"),
        Chain.ARBITRUM: get_config("SUSHI_ARBITRUM_SUBGRAPH_URL"),
        Chain.BASE: get_config("SUSHI_BASE_SUBGRAPH_URL"),
    },
    Protocol.RAMSES: {
        Chain.ARBITRUM: get_config("RAMSES_ARBITRUM_SUBGRAPH_URL"),
    },
    Protocol.ASCENT: {
        Chain.POLYGON: get_config("ASCENT_POLYGON_SUBGRAPH_URL"),
    },
    Protocol.FUSIONX: {
        Chain.MANTLE: get_config("FUSIONX_MANTLE_SUBGRAPH_URL"),
    },
    Protocol.SYNTHSWAP: {
        Chain.BASE: get_config("SYNTHSWAP_BASE_SUBGRAPH_URL"),
    },
    Protocol.LYNEX: {
        Chain.LINEA: get_config("LYNEX_LINEA_SUBGRAPH_URL"),
    },
    Protocol.PEGASYS: {
        Chain.ROLLUX: get_config("PEGASYS_ROLLUX_SUBGRAPH_URL"),
    },
    Protocol.BASEX: {
        Chain.BASE: get_config("BASEX_BASE_SUBGRAPH_URL"),
    },
    Protocol.PANCAKESWAP: {
        Chain.ARBITRUM: get_config("PANCAKESWAP_ARBITRUM_SUBGRAPH_URL"),
    },
    Protocol.APERTURE: {
        Chain.MANTA: get_config("APERTURE_MANTA_SUBGRAPH_URL"),
    },
    Protocol.HERCULES: {
        Chain.METIS: get_config("HERCULES_METIS_SUBGRAPH_URL"),
    },
    Protocol.BASESWAP: {
        Chain.BASE: get_config("BASESWAP_BASE_SUBGRAPH_URL"),
    },
    Protocol.SWAPBASED: {
        Chain.BASE: get_config("SWAPBASED_BASE_SUBGRAPH_URL"),
    },
    Protocol.PHARAOH: {
        Chain.AVALANCHE: get_config("PHARAOH_AVALANCHE_SUBGRAPH_URL"),
    },
    Protocol.SWAPR: {
        Chain.GNOSIS: get_config("SWAPR_GNOSIS_SUBGRAPH_URL"),
    },
    Protocol.THICK: {
        Chain.BASE: get_config("THICK_BASE_SUBGRAPH_URL"),
    },
    Protocol.CLEOPATRA: {
        Chain.MANTLE: get_config("CLEOPATRA_MANTLE_SUBGRAPH_URL"),
    },
    Protocol.BLASTER: {
        Chain.BLAST: get_config("BLASTER_BLAST_SUBGRAPH_URL"),
    },
}

DEX_HYPEPOOL_SUBGRAPH_URLS = {
    Protocol.UNISWAP: {
        Chain.ETHEREUM: get_config("UNISWAP_MAINNET_HP_SUBGRAPH_URL"),
        Chain.POLYGON: get_config("UNISWAP_POLYGON_HP_SUBGRAPH_URL"),
        Chain.ARBITRUM: get_config("UNISWAP_ARBITRUM_HP_SUBGRAPH_URL"),
        Chain.OPTIMISM: get_config("UNISWAP_OPTIMISM_HP_SUBGRAPH_URL"),
        Chain.CELO: get_config("UNISWAP_CELO_HP_SUBGRAPH_URL"),
        Chain.BSC: get_config("UNISWAP_BSC_HP_SUBGRAPH_URL"),
        Chain.MOONBEAM: get_config("UNISWAP_MOONBEAM_HP_SUBGRAPH_URL"),
        Chain.AVALANCHE: get_config("UNISWAP_AVALANCHE_HP_SUBGRAPH_URL"),
    },
    Protocol.QUICKSWAP: {
        Chain.POLYGON: get_config("QUICKSWAP_POLYGON_HP_SUBGRAPH_URL"),
        Chain.POLYGON_ZKEVM: get_config("QUICKSWAP_POLYGON_ZKEVM_HP_SUBGRAPH_URL"),
        Chain.MANTA: get_config("QUICKSWAP_MANTA_HP_SUBGRAPH_URL"),
        Chain.ASTAR_ZKEVM: get_config("QUICKSWAP_ASTAR_ZKEVM_HP_SUBGRAPH_URL"),
        Chain.IMMUTABLE_ZKEVM: get_config("QUICKSWAP_IMMUTABLE_HP_SUBGRAPH_URL"),
    },
    Protocol.QUICKSWAP_UNISWAP: {
        Chain.POLYGON_ZKEVM: get_config("QUICKSWAP_UNISWAP_POLYGON_ZKEVM_HP_SUBGRAPH_URL"),
    },
    Protocol.ZYBERSWAP: {
        Chain.ARBITRUM: get_config("ZYBERSWAP_ARBITRUM_HP_SUBGRAPH_URL"),
    },
    Protocol.THENA: {
        Chain.BSC: get_config("THENA_BSC_HP_SUBGRAPH_URL"),
        Chain.OPBNB: get_config("THENA_OPBNB_HP_SUBGRAPH_URL"),
    },
    Protocol.CAMELOT: {
        Chain.ARBITRUM: get_config("CAMELOT_ARBITRUM_HP_SUBGRAPH_URL"),
    },
    Protocol.GLACIER: {
        Chain.AVALANCHE: get_config("GLACIER_AVALANCHE_HP_SUBGRAPH_URL"),
    },
    Protocol.RETRO: {
        Chain.POLYGON: get_config("RETRO_POLYGON_HP_SUBGRAPH_URL"),
    },
    Protocol.STELLASWAP: {
        Chain.MOONBEAM: get_config("STELLASWAP_MOONBEAM_HP_SUBGRAPH_URL"),
    },
    Protocol.BEAMSWAP: {
        Chain.MOONBEAM: get_config("BEAMSWAP_MOONBEAM_HP_SUBGRAPH_URL"),
    },
    Protocol.SPIRITSWAP: {
        Chain.FANTOM: get_config("SPIRITSWAP_FANTOM_HP_SUBGRAPH_URL"),
    },
    Protocol.SUSHI: {
        Chain.POLYGON: get_config("SUSHI_POLYGON_HP_SUBGRAPH_URL"),
        Chain.ARBITRUM: get_config("SUSHI_ARBITRUM_HP_SUBGRAPH_URL"),
        Chain.BASE: get_config("SUSHI_BASE_HP_SUBGRAPH_URL"),
    },
    Protocol.RAMSES: {
        Chain.ARBITRUM: get_config("RAMSES_ARBITRUM_HP_SUBGRAPH_URL"),
    },
    Protocol.ASCENT: {
        Chain.POLYGON: get_config("ASCENT_POLYGON_HP_SUBGRAPH_URL"),
    },
    Protocol.FUSIONX: {
        Chain.MANTLE: get_config("FUSIONX_MANTLE_HP_SUBGRAPH_URL"),
    },
    Protocol.SYNTHSWAP: {
        Chain.BASE: get_config("SYNTHSWAP_BASE_HP_SUBGRAPH_URL"),
    },
    Protocol.LYNEX: {
        Chain.LINEA: get_config("LYNEX_LINEA_HP_SUBGRAPH_URL"),
    },
    Protocol.PEGASYS: {
        Chain.ROLLUX: get_config("PEGASYS_ROLLUX_HP_SUBGRAPH_URL"),
    },
    Protocol.BASEX: {
        Chain.BASE: get_config("BASEX_BASE_HP_SUBGRAPH_URL"),
    },
    Protocol.PANCAKESWAP: {
        Chain.ARBITRUM: get_config("PANCAKESWAP_ARBITRUM_HP_SUBGRAPH_URL"),
    },
    Protocol.APERTURE: {
        Chain.MANTA: get_config("APERTURE_MANTA_HP_SUBGRAPH_URL"),
    },
    Protocol.HERCULES: {
        Chain.METIS: get_config("HERCULES_METIS_HP_SUBGRAPH_URL"),
    },
    Protocol.BASESWAP: {
        Chain.BASE: get_config("BASESWAP_BASE_HP_SUBGRAPH_URL"),
    },
    Protocol.SWAPBASED: {
        Chain.BASE: get_config("SWAPBASED_BASE_HP_SUBGRAPH_URL"),
    },
    Protocol.PHARAOH: {
        Chain.AVALANCHE: get_config("PHARAOH_AVALANCHE_HP_SUBGRAPH_URL"),
    },
    Protocol.SWAPR: {
        Chain.GNOSIS: get_config("SWAPR_GNOSIS_HP_SUBGRAPH_URL"),
    },
    Protocol.THICK: {
        Chain.BASE: get_config("THICK_BASE_HP_SUBGRAPH_URL"),
    },
    Protocol.CLEOPATRA: {
        Chain.MANTLE: get_config("CLEOPATRA_MANTLE_HP_SUBGRAPH_URL"),
    },
    Protocol.BLASTER: {
        Chain.BLAST: get_config("BLASTER_BLAST_HP_SUBGRAPH_URL"),
    },
}

GAMMA_SUBGRAPH_URLS = {
    Protocol.UNISWAP: {
        Chain.ETHEREUM: get_config("UNISWAP_MAINNET_GAMMA_SUBGRAPH_URL"),
        Chain.POLYGON: get_config("UNISWAP_POLYGON_GAMMA_SUBGRAPH_URL"),
        Chain.ARBITRUM: get_config("UNISWAP_ARBITRUM_GAMMA_SUBGRAPH_URL"),
        Chain.OPTIMISM: get_config("UNISWAP_OPTIMISM_GAMMA_SUBGRAPH_URL"),
        Chain.CELO: get_config("UNISWAP_CELO_GAMMA_SUBGRAPH_URL"),
        Chain.BSC: get_config("UNISWAP_BSC_GAMMA_SUBGRAPH_URL"),
        Chain.MOONBEAM: get_config("UNISWAP_MOONBEAM_GAMMA_SUBGRAPH_URL"),
        Chain.AVALANCHE: get_config("UNISWAP_AVALANCHE_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.QUICKSWAP: {
        Chain.POLYGON: get_config("QUICKSWAP_POLYGON_GAMMA_SUBGRAPH_URL"),
        Chain.POLYGON_ZKEVM: get_config("QUICKSWAP_POLYGON_ZKEVM_GAMMA_SUBGRAPH_URL"),
        Chain.MANTA: get_config("QUICKSWAP_MANTA_GAMMA_SUBGRAPH_URL"),
        Chain.ASTAR_ZKEVM: get_config("QUICKSWAP_ASTAR_ZKEVM_GAMMA_SUBGRAPH_URL"),
        Chain.IMMUTABLE_ZKEVM: get_config("QUICKSWAP_IMMUTABLE_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.QUICKSWAP_UNISWAP: {
        Chain.POLYGON_ZKEVM: get_config("QUICKSWAP_UNISWAP_POLYGON_ZKEVM_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.ZYBERSWAP: {
        Chain.ARBITRUM: get_config("ZYBERSWAP_ARBITRUM_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.THENA: {
        Chain.BSC: get_config("THENA_BSC_GAMMA_SUBGRAPH_URL"),
        Chain.OPBNB: get_config("THENA_OPBNB_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.CAMELOT: {
        Chain.ARBITRUM: get_config("CAMELOT_ARBITRUM_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.GLACIER: {
        Chain.AVALANCHE: get_config("GLACIER_AVALANCHE_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.RETRO: {
        Chain.POLYGON: get_config("RETRO_POLYGON_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.STELLASWAP: {
        Chain.MOONBEAM: get_config("STELLASWAP_MOONBEAM_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.BEAMSWAP: {
        Chain.MOONBEAM: get_config("BEAMSWAP_MOONBEAM_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.SPIRITSWAP: {
        Chain.FANTOM: get_config("SPIRITSWAP_FANTOM_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.SUSHI: {
        Chain.POLYGON: get_config("SUSHI_POLYGON_GAMMA_SUBGRAPH_URL"),
        Chain.ARBITRUM: get_config("SUSHI_ARBITRUM_GAMMA_SUBGRAPH_URL"),
        Chain.BASE: get_config("SUSHI_BASE_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.RAMSES: {
        Chain.ARBITRUM: get_config("RAMSES_ARBITRUM_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.ASCENT: {
        Chain.POLYGON: get_config("ASCENT_POLYGON_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.FUSIONX: {
        Chain.MANTLE: get_config("FUSIONX_MANTLE_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.SYNTHSWAP: {
        Chain.BASE: get_config("SYNTHSWAP_BASE_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.LYNEX: {
        Chain.LINEA: get_config("LYNEX_LINEA_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.PEGASYS: {
        Chain.ROLLUX: get_config("PEGASYS_ROLLUX_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.BASEX: {
        Chain.BASE: get_config("BASEX_BASE_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.PANCAKESWAP: {
        Chain.ARBITRUM: get_config("PANCAKESWAP_ARBITRUM_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.APERTURE: {
        Chain.MANTA: get_config("APERTURE_MANTA_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.HERCULES: {
        Chain.METIS: get_config("HERCULES_METIS_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.BASESWAP: {
        Chain.BASE: get_config("BASESWAP_BASE_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.SWAPBASED: {
        Chain.BASE: get_config("SWAPBASED_BASE_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.PHARAOH: {
        Chain.AVALANCHE: get_config("PHARAOH_AVALANCHE_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.SWAPR: {
        Chain.GNOSIS: get_config("SWAPR_GNOSIS_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.THICK: {
        Chain.BASE: get_config("THICK_BASE_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.CLEOPATRA: {
        Chain.MANTLE: get_config("CLEOPATRA_MANTLE_GAMMA_SUBGRAPH_URL"),
    },
    Protocol.BLASTER: {
        Chain.BLAST: get_config("BLASTER_BLAST_GAMMA_SUBGRAPH_URL"),
    },
}

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
