from sources.common.general.config import get_config
from sources.common.general.enums import Chain


# load configuration file
CONFIGURATION = {"cache": {"save_path": "data/cache"}}  # load_configuration()
# load rpc providers
CONFIGURATION["WEB3_PROVIDER_URLS"] = get_config("WEB3_PROVIDER_URLS")
CONFIGURATION["WEB3_PROVIDER_DEFAULT_ORDER"] = get_config("WEB3_PROVIDER_DEFAULT_ORDER")

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


WEB3_CHAIN_IDS = {chain.database_name: chain.id for chain in Chain}


STATIC_REGISTRY_ADDRESSES = {
    "ethereum": {
        "hypervisors": {
            "uniswapv3": "0x31ccdb5bd6322483bebd0787e1dabd1bf1f14946".lower(),
        },
        "MasterChefV2Registry": {},
        "feeDistributors": [
            "0x07432C021f0A65857a3Ab608600B9FEABF568EA0".lower(),
            "0x8451122f06616baff7feb10afc2c4f4132fc4709".lower(),
        ],
    },
    "polygon": {
        "hypervisors": {
            "uniswapv3": "0x0Ac4C7b794f3D7e7bF1093A4f179bA792CF15055".lower(),
            "quickswap": "0xAeC731F69Fa39aD84c7749E913e3bC227427Adfd".lower(),
            "retro": "0xcac19d43c9558753d7535978a370055614ce832e".lower(),
            "sushi": "0x97686103b3e7238ca6c2c439146b30adbd84a593".lower(),
        },
        "MasterChefRegistry": "0x135B02F8b110Fe2Dd8B6a5e2892Ee781264c2fbe".lower(),
        "MasterChefV2Registry": {
            "uniswapv3": "0x02C8D3FCE5f072688e156F503Bd5C7396328613A".lower(),
            "quickswap": "0x62cD3612233B2F918BBf0d17B9Eda3005b84e16f".lower(),
            "retro": "0x838f6c0189cd8fd831355b31d71b03373480ab83".lower(),
            "sushi": "0x73cb7b82e43759b637e1eb833b6c2711f3e45dca".lower(),
        },
    },
    "optimism": {
        "hypervisors": {
            "uniswapv3": "0xF5BFA20F4A77933fEE0C7bB7F39E7642A070d599".lower(),
        },
        "MasterChefV2Registry": {
            "uniswapv3": "0x81d9bF667205662bfa729C790F67D97D54EA391C".lower(),
        },
    },
    "arbitrum": {
        "hypervisors": {
            "uniswapv3": "0x66CD859053c458688044d816117D5Bdf42A56813".lower(),
            "zyberswap": "0x37595FCaF29E4fBAc0f7C1863E3dF2Fe6e2247e9".lower(),
            "camelot": "0xa216C2b6554A0293f69A1555dd22f4b7e60Fe907".lower(),
            "sushi": "0x0f867f14b39a5892a39841a03ba573426de4b1d0".lower(),
            "ramses": "0x34Ffbd9Db6B9bD8b095A0d156de69a2AD2944666".lower(),
        },
        "MasterChefV2Registry": {
            "camelot": "0x26da8473AaA54e8c7835fA5fdd1599eB4c144d31".lower(),
            "sushi": "0x5f0589ae3ff36bcd1d7a5b1e5287b1ed65f1a934".lower(),
        },
        "zyberswap_v1_masterchefs": [
            "0x9ba666165867e916ee7ed3a3ae6c19415c2fbddd".lower(),
        ],
    },
    "celo": {
        "hypervisors": {
            "uniswapv3": "0x0F548d7AD1A0CB30D1872b8C18894484d76e1569".lower(),
        },
        "MasterChefV2Registry": {},
    },
    "binance": {
        "hypervisors": {
            "uniswapv3": "0x0b4645179C1b668464Df01362fC6219a7ab3234c".lower(),
            "thena": "0xd4bcFC023736Db5617E5638748E127581d5929bd".lower(),
        },
        "MasterChefV2Registry": {},
    },
    "polygon_zkevm": {
        "hypervisors": {
            "quickswap": "0xD08B593eb3460B7aa5Ce76fFB0A3c5c938fd89b8".lower(),
        },
        "MasterChefV2Registry": {
            "quickswap": "0x5b8F58a33808222d1fF93C919D330cfA5c8e1B7d".lower(),
        },
    },
    "fantom": {
        "hypervisors": {
            "spiritswap": "0xf874d4957861e193aec9937223062679c14f9aca".lower(),
        },
        "MasterChefV2Registry": {
            "spiritswap": "0xf5bfa20f4a77933fee0c7bb7f39e7642a070d599".lower(),
        },
    },
    "moonbeam": {
        "hypervisors": {
            "stellaswap": "0x6002d7714e8038f2058e8162b0b86c0b19c31908".lower(),
            "beamswap": "0xb7dfc304d9cd88d98a262ce5b6a39bb9d6611063".lower(),
        },
        "MasterChefV2Registry": {
            "stellaswap": "0xd08b593eb3460b7aa5ce76ffb0a3c5c938fd89b8".lower(),
            "beamswap": "0x1cc4ee0cb063e9db36e51f5d67218ff1f8dbfa0f".lower(),
        },
    },
}


KNOWN_VALID_MASTERCHEFS = {
    "polygon": {
        "uniswapv3": ["0x570d60a60baa356d47fda3017a190a48537fcd7d"],
        "quickswap": [
            "0x20ec0d06f447d550fc6edee42121bc8c1817b97d",
            "0x68678cf174695fc2d27bd312df67a3984364ffdd",
        ],
    },
    "optimism": {
        "uniswapv3": ["0xc7846d1bc4d8bcf7c45a7c998b77ce9b3c904365"],
    },
}
