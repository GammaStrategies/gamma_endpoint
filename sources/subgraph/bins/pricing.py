"""Token pricing"""

import asyncio
import contextlib
from collections import defaultdict

from gql.dsl import DSLQuery

from sources.common.prices.helpers import get_current_prices
from sources.subgraph.bins import LlamaClient, UniswapV3Client
from sources.subgraph.bins.enums import Chain, Protocol
from sources.subgraph.bins.subgraphs import SubgraphData
from sources.subgraph.bins.subgraphs.gamma import GammaClient
from sources.subgraph.bins.utils import sqrtPriceX96_to_priceDecimal

POOLS = {
    Chain.ETHEREUM: {
        "USDC_WETH": {
            "protocol": Protocol.UNISWAP,
            "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
        },
        "WETH_RPL": {
            "protocol": Protocol.UNISWAP,
            "address": "0xe42318ea3b998e8355a3da364eb9d48ec725eb45",
        },
        "GAMMA_WETH": {
            "protocol": Protocol.UNISWAP,
            "address": "0x4006bed7bf103d70a1c6b7f1cef4ad059193dc25",
        },
        "AXL_USDC": {
            "protocol": Protocol.UNISWAP,
            "address": "0x5b0d2536f0c970b8d9cbf3959460fb97ce808ade",
        },
    },
    Chain.OPTIMISM: {
        "WETH_USDC": {
            "protocol": Protocol.UNISWAP,
            "address": "0x85149247691df622eaf1a8bd0cafd40bc45154a9",
        },
        "WETH_OP": {
            "protocol": Protocol.UNISWAP,
            "address": "0x68f5c0a2de713a54991e01858fd27a3832401849",
        },
    },
    Chain.POLYGON: {
        "WMATIC_USDC": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0xae81fac689a1b4b1e06e7ef4a2ab4cd8ac0a087d",
        },
        "WMATIC_QI": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0x5cd94ead61fea43886feec3c95b1e9d7284fdef3",
        },
        "WMATIC_QUICK": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0x9f1a8caf3c8e94e43aa64922d67dff4dc3e88a42",
        },
        "WMATIC_DQUICK": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0xb8d00c66accdc01e78fd7957bf24050162916ae2",
        },
        "WMATIC_GHST": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0x80deece4befd9f27d2df88064cf75f080d3ce1b2",
        },
        "WMATIC_ANKR": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0x2f2dd65339226df7441097a710aba0f493879579",
        },
        "USDC_DAVOS": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0xfb0bc232cd11dbe804b489860c470b7f9cc80d9f",
        },
        "USDC_GIDDY": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0x65c30f39b880bdd9616280450c4b41cc74b438b7",
        },
        "WMATIC_LCD": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0xd9c2c978915b907df04972cb3f577126fe55143c",
        },
        "WOMBAT_USDC": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0xaf835698673655e9910de8398df6c5238f5d3aeb",
        },
        "USDC_FIS": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0x2877703a3ba3e712d684d22bd6d60cc0031d84e8",
        },
        "SD_USDC": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0x5d0acfa39a0fca603147f1c14e53f46be76984bc",
        },
        "USDC_DAI": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0xe7e0eb9f6bcccfe847fdf62a3628319a092f11a2",
        },
        "WETH_FBX": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0x17b509b2b65b0d07b9e46bfc2ffe6c9c09a8e821",
        },
        "USDC_WETH": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0x55caabb0d2b704fd0ef8192a7e35d8837e678207",
        },
        "ANKRMATIC_ANKR": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0xe629eb79d27f727747c80ccb937e3a51fbacfd4d",
        },
        "WMATIC_ANKRMATIC": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0x5baaffa2cb0f71af28a1bd9dcfbb98c95b52fb20",
        },
        "PUSH_WETH": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0xf66066175bc4dcbcb7ee6e01becd8489b6eeb344",
        },
        "WMATIC_RUNY": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0x7935aefec4077611ebb088f640de9462d39cc460",
        },
    },
    Chain.POLYGON_ZKEVM: {
        "WETH_USDC": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0xc44ad482f24fd750caeba387d2726d8653f8c4bb",
        },
        "QUICK_USDC": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0x1247b70c4b41890e8c1836e88dd0c8e3b23dd60e",
        },
        "WETH_MATIC": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0xb73abfb5a2c89f4038baa476ff3a7942a021c196",
        },
        "FXS_FRAX": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0xcbb9d995933c4f7a8ceb0c7cb096cb9b7d9defc8",
        },
        "USDC_FRAX": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0xc4ad89d0a07081871f3007079f816b0757d2638e",
        },
        "PUSH_WETH": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0xf66066175bc4dcbcb7ee6e01becd8489b6eeb344",
        },
        "DUSD_USDC": {
            "protocol": Protocol.QUICKSWAP,
            "address": "0xf63aaf0c7e35742a888a84807736ae5d989aa206",
        },
    },
    Chain.BSC: {},
    Chain.AVALANCHE: {
        "GLCR_USDC": {
            "protocol": Protocol.GLACIER,
            "address": "0x5de8128b5f49ed6bdac9fa9b8661e9d3bb9334da",
        }
    },
    Chain.ARBITRUM: {
        "DAI_USDC": {
            "protocol": Protocol.UNISWAP,
            "address": "0xf0428617433652c9dc6d1093a42adfbf30d29f74",
        },
        "WETH_NOISEGPT": {
            "protocol": Protocol.UNISWAP,
            "address": "0xda5660ff6de514eecfa20a39160db6ef671f996f",
        },
        "WETH_USDC": {
            "protocol": Protocol.UNISWAP,
            "address": "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443",
        },
        "ARB_USDC": {
            "protocol": Protocol.UNISWAP,
            "address": "0xcda53b1f66614552f834ceef361a8d12a0b8dad8",
        },
        "CAKE_WETH": {
            "protocol": Protocol.PANCAKESWAP,
            "address": "0xf5fac36c2429e1cf84d4abacdb18477ef32589c9",
        },
    },
    Chain.MOONBEAM: {
        "USDC_WGLMR": {
            "protocol": Protocol.STELLASWAP,
            "address": "0xab8c35164a8e3ef302d18da953923ea31f0fe393",
        }
    },
    Chain.FANTOM: {},
    Chain.BASE: {},
    Chain.ROLLUX: {},
}


POOL_PATHS = {
    Chain.ETHEREUM: {
        # GAMMA
        "0x6bea7cfef803d1e3d5f7c0103f7ded065644e197": [
            (POOLS[Chain.ETHEREUM]["GAMMA_WETH"], 1),
            (POOLS[Chain.ETHEREUM]["USDC_WETH"], 0),
        ],
        # RPL
        "0xd33526068d116ce69f19a9ee46f0bd304f21a51f": [
            (POOLS[Chain.ETHEREUM]["WETH_RPL"], 0),
            (POOLS[Chain.ETHEREUM]["USDC_WETH"], 0),
        ],
        # AXL
        "0x467719ad09025fcc6cf6f8311755809d45a5e5f3": [
            (POOLS[Chain.ETHEREUM]["AXL_USDC"], 1)
        ],
    },
    Chain.OPTIMISM: {
        # OP
        "0x4200000000000000000000000000000000000042": [
            (POOLS[Chain.OPTIMISM]["WETH_OP"], 0),
            (POOLS[Chain.OPTIMISM]["WETH_USDC"], 1),
        ],
        # MOCK-OPT
        "0x601e471de750cdce1d5a2b8e6e671409c8eb2367": [
            (POOLS[Chain.OPTIMISM]["WETH_OP"], 0),
            (POOLS[Chain.OPTIMISM]["WETH_USDC"], 1),
        ],
    },
    Chain.POLYGON: {
        # USDC
        "0x2791bca1f2de4661ed88a30c99a7a9449aa84174": [],
        # WMATIC
        "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270": [
            (POOLS[Chain.POLYGON]["WMATIC_USDC"], 1)
        ],
        # QI
        "0x580a84c73811e1839f75d86d75d88cca0c241ff4": [
            (POOLS[Chain.POLYGON]["WMATIC_QI"], 0),
            (POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # QUICK
        "0xb5c064f955d8e7f38fe0460c556a72987494ee17": [
            (POOLS[Chain.POLYGON]["WMATIC_QUICK"], 0),
            (POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # dQUICK
        "0x958d208cdf087843e9ad98d23823d32e17d723a1": [
            (POOLS[Chain.POLYGON]["WMATIC_DQUICK"], 0),
            (POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # GHST
        "0x385eeac5cb85a38a9a07a70c73e0a3271cfb54a7": [
            (POOLS[Chain.POLYGON]["WMATIC_GHST"], 0),
            (POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # ANKR
        "0x101a023270368c0d50bffb62780f4afd4ea79c35": [
            (POOLS[Chain.POLYGON]["ANKRMATIC_ANKR"], 0),
            (POOLS[Chain.POLYGON]["WMATIC_ANKRMATIC"], 0),
            (POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # DAVOS
        "0xec38621e72d86775a89c7422746de1f52bba5320": [
            (POOLS[Chain.POLYGON]["USDC_DAVOS"], 0)
        ],
        # GIDDY
        "0x67eb41a14c0fe5cd701fc9d5a3d6597a72f641a6": [
            (POOLS[Chain.POLYGON]["USDC_GIDDY"], 0)
        ],
        # LCD
        "0xc2a45fe7d40bcac8369371b08419ddafd3131b4a": [
            (POOLS[Chain.POLYGON]["WMATIC_LCD"], 0),
            (POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
        # WOMBAT
        "0x0c9c7712c83b3c70e7c5e11100d33d9401bdf9dd": [
            (POOLS[Chain.POLYGON]["WOMBAT_USDC"], 1),
        ],
        # FIS
        "0x7a7b94f18ef6ad056cda648588181cda84800f94": [
            (POOLS[Chain.POLYGON]["USDC_FIS"], 0),
        ],
        # SD
        "0x1d734a02ef1e1f5886e66b0673b71af5b53ffa94": [
            (POOLS[Chain.POLYGON]["SD_USDC"], 1),
        ],
        # DAI
        "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063": [
            (POOLS[Chain.POLYGON]["USDC_DAI"], 0),
        ],
        # FBX
        "0xd125443f38a69d776177c2b9c041f462936f8218": [
            (POOLS[Chain.POLYGON]["WETH_FBX"], 0),
            (POOLS[Chain.POLYGON]["USDC_WETH"], 0),
        ],
        # PUSH
        "0x58001cc1a9e17a20935079ab40b1b8f4fc19efd1": [
            (POOLS[Chain.POLYGON]["PUSH_WETH"], 1),
            (POOLS[Chain.POLYGON]["USDC_WETH"], 0),
        ],
        # RUNY
        "0x578fee9def9a270c20865242cfd4ff86f31d0e5b": [
            (POOLS[Chain.POLYGON]["WMATIC_RUNY"], 0),
            (POOLS[Chain.POLYGON]["WMATIC_USDC"], 1),
        ],
    },
    Chain.POLYGON_ZKEVM: {
        # WMATIC
        "0xa2036f0538221a77a3937f1379699f44945018d0": [
            (POOLS[Chain.POLYGON_ZKEVM]["WETH_MATIC"], 0),
            (POOLS[Chain.POLYGON_ZKEVM]["WETH_USDC"], 1),
        ],
        # QUICK
        "0x68286607a1d43602d880d349187c3c48c0fd05e6": [
            (POOLS[Chain.POLYGON_ZKEVM]["QUICK_USDC"], 1),
        ],
        # FRAX
        "0x6b856a14cea1d7dcfaf80fa6936c0b75972ccace": [
            (POOLS[Chain.POLYGON_ZKEVM]["FXS_FRAX"], 1),
            (POOLS[Chain.POLYGON_ZKEVM]["USDC_FRAX"], 0),
        ],
        # DUSD
        "0x819d1daa794c1c46b841981b61cc978d95a17b8e": [
            (POOLS[Chain.POLYGON_ZKEVM]["DUSD_USDC"], 1),
        ],
    },
    Chain.BSC: {},
    Chain.AVALANCHE: {
        # GLCR
        "0x3712871408a829c5cd4e86da1f4ce727efcd28f6": [
            (POOLS[Chain.AVALANCHE]["GLCR_USDC"], 1),
        ],
    },
    Chain.ARBITRUM: {
        # DAI
        "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": [
            (POOLS[Chain.ARBITRUM]["DAI_USDC"], 1),
        ],
        # NOISEGPT
        "0xadd5620057336f868eae78a451c503ae7b576bad": [
            (POOLS[Chain.ARBITRUM]["WETH_NOISEGPT"], 0),
            (POOLS[Chain.ARBITRUM]["WETH_USDC"], 1),
        ],
        # ARB
        "0x912ce59144191c1204e64559fe8253a0e49e6548": [
            (POOLS[Chain.ARBITRUM]["ARB_USDC"], 1),
        ],
        # Cake
        "0x1b896893dfc86bb67cf57767298b9073d2c1ba2c": [
            (POOLS[Chain.ARBITRUM]["CAKE_WETH"], 1),
            (POOLS[Chain.ARBITRUM]["WETH_USDC"], 1),
        ],
    },
    Chain.MOONBEAM: {
        # WGLMR
        "0xacc15dc74880c9944775448304b263d191c6077f": [
            (POOLS[Chain.MOONBEAM]["USDC_WGLMR"], 0),
        ]
    },
    Chain.FANTOM: {},
    Chain.BASE: {},
    Chain.ROLLUX: {},
}


class DexPriceData:
    """Base class for dex prices"""

    def __init__(self, protocol: Protocol, chain: Chain, pools: list[str]) -> None:
        self.protocol = protocol
        self.chain = chain
        self.uniswap_client = UniswapV3Client(protocol, chain)
        self.pools = pools
        self.pool_query = ""
        self.data = {}

    def _init_queries(self):
        self.pool_query = """
        query tokenPrice($pools: [String!]!){
            pools(
                where: {
                    id_in: $pools
                }
            ){
                id
                sqrtPrice
                token0{
                    symbol
                    decimals
                }
                token1{
                    symbol
                    decimals
                }
            }
        }
        """

    async def get_data(self):
        """Get DEX price data from subgraph"""
        self._init_queries()
        variables = {"pools": self.pools}
        response = await self.uniswap_client.query(self.pool_query, variables)
        self.data = response.get("data", {}).get("pools", {})


class DexPrice:
    """Class for getting prices from DEXes"""

    def __init__(self, chain: Chain):
        self.chain_prices: dict
        self.token_prices: dict
        self.chain = chain

    async def _get_data(self):
        pools_by_protocol = defaultdict(list)
        for pool in POOLS.get(self.chain, {}).values():
            pools_by_protocol[pool["protocol"]].append(pool["address"])

        dex_clients = [
            DexPriceData(protocol, self.chain, pools)
            for protocol, pools in pools_by_protocol.items()
        ]

        await asyncio.gather(*[client.get_data() for client in dex_clients])

        chain_prices = {
            protocol_client.protocol: {
                pool.pop("id"): pool for pool in protocol_client.data
            }
            for protocol_client in dex_clients
        }

        self.chain_prices = chain_prices

    async def get_token_prices(self):
        """Get all defined token prices for the chain"""
        await self._get_data()
        token_pricing = {}
        for token, path in POOL_PATHS.get(self.chain, {}).items():
            price = 1
            for pool in path:
                pool_address = pool[0]["address"]
                pool_protocol = pool[0]["protocol"]
                pool_info = self.chain_prices[pool_protocol].get(pool_address)

                if not pool_info:
                    price = 0
                    break

                sqrt_price_x96 = float(pool_info["sqrtPrice"])
                decimal0 = int(pool_info["token0"]["decimals"])
                decimal1 = int(pool_info["token1"]["decimals"])

                token_in_base = sqrtPriceX96_to_priceDecimal(
                    sqrt_price_x96, decimal0, decimal1
                )
                if pool[1] == 0:
                    token_in_base = 1 / token_in_base

                price *= token_in_base

            token_pricing[token] = price

        self.token_prices = token_pricing


async def gamma_price():
    """Get price of GAMMA"""
    dex_pricing = DexPrice(Chain.ETHEREUM)
    await dex_pricing.get_token_prices()
    return dex_pricing.token_prices["0x6bea7cfef803d1e3d5f7c0103f7ded065644e197"]


async def token_prices(chain: Chain, protocol: Protocol) -> dict:
    """Get token prices"""
    dex_pricing = DexPrice(chain)
    token_data = TokenData(chain, protocol)

    await asyncio.gather(
        token_data.get_data(),  # get list of tokens
        dex_pricing.get_token_prices(),  # Get prices from subgraph
    )
    dex_prices = dex_pricing.token_prices

    # Get prices from defillama and database
    llama_client = LlamaClient(chain)

    llama_prices, db_token_prices = await asyncio.gather(
        llama_client.current_token_price_multi(token_data.data),
        get_current_prices(chain, token_data.data),
    )

    # Base case pricing from DB
    prices = {token["address"]: token["price"] for token in db_token_prices}

    # Find tokens that were not priced in DB
    unpriced_tokens = token_data.data - prices.keys()

    for token in unpriced_tokens:
        prices[token] = llama_prices.get(token, dex_prices.get(token, 0))

    with contextlib.suppress(Exception):
        if chain == Chain.POLYGON:
            T_MAINNET = "0xcdf7028ceab81fa0c6971208e83fa7872994bee5"
            T_POLYGON = "0x1d0ab64ed0f1ee4a886462146d26effc6dd00d0b"

            AXL_MAINNET = "0x467719ad09025fcc6cf6f8311755809d45a5e5f3"
            AXL_POLYGON = "0x6e4e624106cb12e168e6533f8ec7c82263358940"

            llama_client_mainnet = LlamaClient(Chain.ETHEREUM)
            prices_mainnet = await llama_client_mainnet.current_token_price_multi(
                [T_MAINNET, AXL_MAINNET]
            )
            prices[T_POLYGON] = prices_mainnet.get(T_MAINNET, 0)
            prices[AXL_POLYGON] = prices_mainnet.get(AXL_MAINNET, 0)

        if chain == Chain.OPTIMISM:
            OP_ADDRESS_OPTIMISM = "0x4200000000000000000000000000000000000042"
            MOCK_OPT_ADDRESS_OPTIMISM = "0x601e471de750cdce1d5a2b8e6e671409c8eb2367"

            prices[MOCK_OPT_ADDRESS_OPTIMISM] = prices.get(OP_ADDRESS_OPTIMISM, 0)

    return prices


class TokenData(SubgraphData):
    """Class to get xGamma staking relateed data"""

    def __init__(self, chain: Chain, protocol: Protocol):
        super().__init__()
        self.data: {}
        self.client = GammaClient(protocol, chain)

    async def _query_data(self) -> dict:
        ds = self.client.data_schema

        query = DSLQuery(
            ds.Query.uniswapV3Hypervisors(first=1000).select(
                ds.UniswapV3Hypervisor.pool.select(
                    ds.UniswapV3Pool.token0.select(ds.Token.id),
                    ds.UniswapV3Pool.token1.select(ds.Token.id),
                )
            ),
            ds.Query.masterChefV2Rewarders(first=1000).select(
                ds.MasterChefV2Rewarder.rewardToken.select(ds.Token.id)
            ),
        )

        response = await self.client.execute(query)
        self.query_response = response

    def _transform_data(self) -> dict:
        hype_tokens = []
        for hype in self.query_response["uniswapV3Hypervisors"]:
            hype_tokens.append(hype["pool"]["token0"]["id"])
            hype_tokens.append(hype["pool"]["token1"]["id"])

        mcv2_tokens = [
            rewarder["rewardToken"]["id"]
            for rewarder in self.query_response["masterChefV2Rewarders"]
        ]

        all_tokens = hype_tokens + mcv2_tokens
        all_tokens = list(set(all_tokens))

        return all_tokens
