from dataclasses import dataclass


@dataclass
class twa_user:
    user_address: str
    start_block_number: int
    end_block_number: int
