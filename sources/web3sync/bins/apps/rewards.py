from sources.common.general.enums import Chain, Protocol
from ..w3.builders import build_hypervisor


async def get_rewards(
    chain: Chain, protocol: Protocol, hypervisor_address: str,  block: int | None = None
):
    result = []


# ramses specific
async def get_ramses_reward_apr(
    chain: Chain,
    hypervisor_address: str,
    block: int | None = None,
    period: int | None = None,
):
    if hypervisor := build_hypervisor(
        network=chain.database_name,
        protocol=Protocol.RAMSES,
        block=block or 0,
        hypervisor_address=hypervisor_address,
    ):
        return {
            rewardToken: hypervisor.calculate_rewards(
                period=period or hypervisor.current_period, reward_token=rewardToken
            )
            for rewardToken in hypervisor.getRewardTokens
        }

