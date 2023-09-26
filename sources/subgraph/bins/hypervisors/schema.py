from dataclasses import dataclass, field, InitVar
from sources.subgraph.bins.schema import TokenPair


@dataclass
class HypervisorToken:
    """Representation of token in hypervisors"""

    address: str
    symbol: str
    decimals: int

    def __post_init__(self):
        self.decimals = int(self.decimals)


@dataclass
class Hypervisor:
    """Hypervisor key fields"""

    address: str
    created: int
    base_lower: int
    base_upper: int
    total_supply: int
    max_total_supply: int
    deposit_max: TokenPair = field(init=False)
    gross_fees_claimed: TokenPair = field(init=False)
    gross_fees_claimed_usd: float
    fees_reinvested: TokenPair = field(init=False)
    fees_reinvested_usd: float
    tvl: TokenPair = field(init=False)
    tvl_usd: float
    pool: str
    pool_fee: int
    pool_price: int
    token_0: HypervisorToken = field(init=False)
    token_1: HypervisorToken = field(init=False)
    token_0_address: InitVar[str]
    token_0_symbol: InitVar[str]
    token_0_decimals: InitVar[int]
    token_1_address: InitVar[str]
    token_1_symbol: InitVar[str]
    token_1_decimals: InitVar[int]
    deposit_max_0: InitVar[int]
    deposit_max_1: InitVar[int]
    gross_fees_claimed_0: InitVar[int]
    gross_fees_claimed_1: InitVar[int]
    fees_reinvested_0: InitVar[int]
    fees_reinvested_1: InitVar[int]
    tvl_0: InitVar[int]
    tvl_1: InitVar[int]

    def __post_init__(
        self,
        token_0_address: str,
        token_0_symbol: str,
        token_0_decimals: int,
        token_1_address: str,
        token_1_symbol: str,
        token_1_decimals: int,
        deposit_max_0: int,
        deposit_max_1: int,
        gross_fees_claimed_0: int,
        gross_fees_claimed_1: int,
        fees_reinvested_0: int,
        fees_reinvested_1: int,
        tvl_0: int,
        tvl_1: int,
    ):
        self.created = int(self.created)
        self.base_lower = int(self.base_lower)
        self.base_upper = int(self.base_upper)
        self.total_supply = int(self.total_supply)
        self.max_total_supply = int(self.max_total_supply)
        self.deposit_max = TokenPair(
            deposit_max_0, deposit_max_1, token_0_decimals, token_1_decimals
        )
        self.gross_fees_claimed = TokenPair(
            gross_fees_claimed_0,
            gross_fees_claimed_1,
            token_0_decimals,
            token_1_decimals,
        )
        self.gross_fees_claimed_usd = float(self.gross_fees_claimed_usd)
        self.fees_reinvested = TokenPair(
            fees_reinvested_0,
            fees_reinvested_1,
            token_0_decimals,
            token_1_decimals,
        )
        self.fees_reinvested_usd = float(self.fees_reinvested_usd)
        self.tvl = TokenPair(tvl_0, tvl_1, token_0_decimals, token_1_decimals)
        self.tvl_usd = float(self.tvl_usd)
        self.pool_fee = int(self.pool_fee)
        self.pool_price = int(self.pool_price)

        self.token_0 = HypervisorToken(
            token_0_address, token_0_symbol, token_0_decimals
        )
        self.token_1 = HypervisorToken(
            token_1_address, token_1_symbol, token_1_decimals
        )
