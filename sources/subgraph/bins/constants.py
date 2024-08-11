from sources.subgraph.bins.enums import Chain

WETH_ADDRESS = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
GAMMA_ADDRESS = "0x6bea7cfef803d1e3d5f7c0103f7ded065644e197"
XGAMMA_ADDRESS = "0x26805021988f1a45dc708b5fb75fc75f21747d8c"

X128 = 2**128

DAYS_IN_PERIOD = {"daily": 1, "weekly": 7, "monthly": 30, "allTime": 2000}

SECONDS_IN_DAYS = 3600

DAY_SECONDS = 24 * 60 * 60
YEAR_SECONDS = 365 * DAY_SECONDS

BLOCK_TIME_SECONDS = {
    Chain.ETHEREUM: 12,
    Chain.POLYGON: 2,
    Chain.OPTIMISM: 1,
    Chain.ARBITRUM: 1,
    Chain.CELO: 5,
    Chain.BSC: 3,
    Chain.POLYGON_ZKEVM: 10,
    Chain.AVALANCHE: 3,
    Chain.FANTOM: 1,
    Chain.MOONBEAM: 12,
    Chain.MANTLE: 10,
    Chain.BASE: 2,
    Chain.LINEA: 12,
    Chain.ROLLUX: 2,
    Chain.MANTA: 12,
    Chain.METIS: 3.6,
    Chain.OPBNB: 1,
    Chain.GNOSIS: 5,
    Chain.ASTAR_ZKEVM: 3,
    Chain.IMMUTABLE_ZKEVM: 2,
    Chain.BLAST: 2,
    Chain.SCROLL: 3,
    Chain.XLAYER: 3,
    Chain.MODE: 2,
    Chain.ROOTSTOCK: 30,
    Chain.TAIKO: 24,
    Chain.SEI: 0.4,
    Chain.IOTA: 10,
    Chain.ZIRCUIT: 2,
}
