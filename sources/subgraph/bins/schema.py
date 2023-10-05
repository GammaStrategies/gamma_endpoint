from dataclasses import InitVar, dataclass, field


@dataclass
class ValueWithDecimal:
    raw: int
    adjusted: float = field(init=False)
    decimals: int

    def __post_init__(self):
        self.raw = int(self.raw)
        self.decimals = int(self.decimals)
        self.adjusted = self.raw / 10**self.decimals


@dataclass
class TokenPair:
    value0: ValueWithDecimal = field(init=False)
    value1: ValueWithDecimal = field(init=False)
    raw0: InitVar[int]
    raw1: InitVar[int]
    decimals0: InitVar[int]
    decimals1: InitVar[int]

    def __post_init__(self, raw0: int, raw1: int, decimals0: int, decimals1: int):
        self.value0 = ValueWithDecimal(raw=raw0, decimals=decimals0)
        self.value1 = ValueWithDecimal(raw=raw1, decimals=decimals1)
