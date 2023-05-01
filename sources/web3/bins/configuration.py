from sources.web3.bins.general.general_utilities import (
    load_configuration,
    check_configuration_file,
)


# load configuration file
CONFIGURATION = {"cache": {"save_path": "data/cache"}}  # load_configuration()


# check configuration
# check_configuration_file(CONFIGURATION)

# add cml_parameters into loaded config ( this is used later on to load again the config file to be able to update on-the-fly vars)
if "_custom_" not in CONFIGURATION.keys():
    CONFIGURATION["_custom_"] = {}
CONFIGURATION["_custom_"]["cml_parameters"] = {}


# add temporal variables while the app is running so memory is kept
CONFIGURATION["_custom_"]["temporal_memory"] = {}


def add_to_memory(key, value):
    """Add to temporal memory a key and value"""
    if key not in CONFIGURATION["_custom_"]["temporal_memory"]:
        CONFIGURATION["_custom_"]["temporal_memory"][key] = []

    if value not in CONFIGURATION["_custom_"]["temporal_memory"][key]:
        CONFIGURATION["_custom_"]["temporal_memory"][key].append(value)


def get_from_memory(key) -> list:
    """Get value from temporal memory"""
    try:
        return CONFIGURATION["_custom_"]["temporal_memory"][key]
    except KeyError:
        return []


#### ADD STATIC CONFIG HERE ####

WEB3_CHAIN_IDS = {
    "ethereum": 1,
    "polygon": 137,
    "optimism": 10,
    "arbitrum": 42161,
    "celo": 42220,
    "binance": 56,
}


STATIC_REGISTRY_ADDRESSES = {
    "ethereum": {
        "hypervisors": {
            "uniswap": "0x31ccdb5bd6322483bebd0787e1dabd1bf1f14946",
        },
        "MasterChefV2Registry": {},
        "feeDistributors": [
            "0x07432C021f0A65857a3Ab608600B9FEABF568EA0",
            "0x8451122f06616baff7feb10afc2c4f4132fc4709",
        ],
    },
    "polygon": {
        "hypervisors": {
            "uniswap": "0x0Ac4C7b794f3D7e7bF1093A4f179bA792CF15055",
            "quickswap": "0xAeC731F69Fa39aD84c7749E913e3bC227427Adfd",
        },
        "MasterChefRegistry": "0x135B02F8b110Fe2Dd8B6a5e2892Ee781264c2fbe",
        "MasterChefV2Registry": {
            "uniswap": "0x02C8D3FCE5f072688e156F503Bd5C7396328613A",
            "quickswap": "0x62cD3612233B2F918BBf0d17B9Eda3005b84e16f",
        },
    },
    "optimism": {
        "hypervisors": {
            "uniswap": "0xF5BFA20F4A77933fEE0C7bB7F39E7642A070d599",
        },
        "MasterChefV2Registry": {
            "uniswap": "0x81d9bF667205662bfa729C790F67D97D54EA391C",
        },
    },
    "arbitrum": {
        "hypervisors": {
            "uniswap": "0x66CD859053c458688044d816117D5Bdf42A56813",
            "zyberswap": "0x37595FCaF29E4fBAc0f7C1863E3dF2Fe6e2247e9",
        },
        "MasterChefV2Registry": {},
        "zyberswap_v1_masterchefs": [
            "0x9ba666165867e916ee7ed3a3ae6c19415c2fbddd",
        ],
    },
    "celo": {
        "hypervisors": {
            "uniswap": "0x0F548d7AD1A0CB30D1872b8C18894484d76e1569",
        },
        "MasterChefV2Registry": {},
    },
    "binance": {
        "hypervisors": {
            "thena": "0xd4bcFC023736Db5617E5638748E127581d5929bd",
        },
        "MasterChefV2Registry": {},
    },
}


RPC_URLS = {
    "ethereum": [
        # "https://mainnet.infura.io/v3/",
        "https://api.mycryptoapi.com/eth",
        "https://cloudflare-eth.com",
        "https://ethereum.publicnode.com",
    ],
    "polygon": [
        # "https://polygon-rpc.com/",
        "https://rpc-mainnet.matic.network",
        "https://matic-mainnet.chainstacklabs.com",
        "https://rpc-mainnet.maticvigil.com",
        "https://rpc-mainnet.matic.quiknode.pro",
        # "https://matic-mainnet-full-rpc.bwarelabs.com",
        "https://polygon-bor.publicnode.com",
    ],
    "optimism": ["https://mainnet.optimism.io/"],
    "arbitrum": [
        # "https://arbitrum-mainnet.infura.io/v3/",
        # "https://arb-mainnet.g.alchemy.com/v2/",
        "https://arb1.arbitrum.io/rpc",
    ],
    "celo": [
        "https://forno.celo.org",
    ],
    "binance": [
        "https://bsc-dataseed1.binance.org",
        "https://bsc-dataseed2.binance.org",
        "https://bsc-dataseed3.binance.org",
        "https://bsc-dataseed4.binance.org",
        "https://bsc-dataseed1.defibit.io",
        "https://bsc-dataseed2.defibit.io",
        "https://bsc-dataseed3.defibit.io",
        "https://bsc-dataseed4.defibit.io",
        "https://bsc-dataseed1.ninicoin.io",
        "https://bsc-dataseed2.ninicoin.io",
        "https://bsc-dataseed3.ninicoin.io",
        "https://bsc-dataseed4.ninicoin.io",
        "https://bsc.publicnode.com",
    ],
}
