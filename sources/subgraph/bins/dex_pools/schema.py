from dataclasses import dataclass

@dataclass
class DexPool:
    address: str
    sqrt_price: int
    tick: int
    observation_index: int
    fees_usd: float
    total_value_locked_usd: float

    def __post_init__(self):
        self.sqrt_price = int(self.sqrt_price)
        self.tick = int(self.tick)
        self.observation_index = int(self.observation_index)
        self.fees_usd = float(self.fees_usd)
        self.total_value_locked_usd = float(self.total_value_locked_usd)
