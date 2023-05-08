import contextlib
import logging
from web3 import Web3
from sources.web3.bins.w3.objects.basic import web3wrap


# Gamma rewarder
class gamma_masterchef_rewarder(web3wrap):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "masterchef_rewarder"
        self._abi_path = abi_path or "sources/common/abis/gamma/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

        self._acc_token_precision: int | None = None
        self._masterchef_v2: str | None = None
        self._funder: str | None = None
        self._owner: str | None = None
        self._pendingOwner: str | None = None
        self._poolLength: int | None = None
        self._rewardPerSecond: int | None = None
        self._rewardToken: str | None = None
        self._totalAllocPoint: int | None = None

    @property
    async def acc_token_precision(self) -> int:
        if not self._acc_token_precision:
            self._acc_token_precision = await self.call_function_autoRpc(
                "ACC_TOKEN_PRECISION"
            )

        return self._acc_token_precision

    @property
    async def masterchef_v2(self) -> str:
        if not self._masterchef_v2:
            self._masterchef_v2 = await self.call_function_autoRpc("MASTERCHEF_V2")
        return self._masterchef_v2

    @property
    async def funder(self) -> str:
        if not self._funder:
            self._funder = await self.call_function_autoRpc("funder")
        return self._funder

    @property
    async def owner(self) -> str:
        if not self._owner:
            self._owner = await self.call_function_autoRpc("owner")
        return self._owner

    @property
    async def pendingOwner(self) -> str:
        if not self._pendingOwner:
            self._pendingOwner = await self.call_function_autoRpc("pendingOwner")
        return self._pendingOwner

    async def pendingToken(self, pid: int, user: str) -> int:
        return await self.call_function_autoRpc("pendingToken", None, pid, user)

    async def pendingTokens(self, pid: int, user: str, input: int) -> tuple[list, list]:
        # rewardTokens address[], rewardAmounts uint256[]
        return await self.call_function_autoRpc("pendingToken", None, pid, user, input)

    async def poolIds(self, input: int) -> int:
        return await self.call_function_autoRpc("poolIds", None, input)

    async def poolInfo(self, input: int) -> tuple[int, int, int]:
        """_summary_

        Args:
            input (int): _description_

        Returns:
            tuple[int, int, int]:  accSushiPerShare uint128, lastRewardTime uint64, allocPoint uint64
                accSushiPerShare — accumulated SUSHI per share, times 1e12.
                lastRewardBlock — number of block, when the reward in the pool was the last time calculated
                allocPoint — allocation points assigned to the pool. SUSHI to distribute per block per pool = SUSHI per block * pool.allocPoint / totalAllocPoint
        """
        return await self.call_function_autoRpc("poolInfo", None, input)

    @property
    async def poolLength(self) -> int:
        if not self._poolLength:
            self._poolLength = await self.call_function_autoRpc("poolLength")
        return self._poolLength

    @property
    async def rewardPerSecond(self) -> int:
        if not self._rewardPerSecond:
            self._rewardPerSecond = await self.call_function_autoRpc("rewardPerSecond")
        return self._rewardPerSecond

    @property
    async def rewardToken(self) -> str:
        if not self._rewardToken:
            self._rewardToken = await self.call_function_autoRpc("rewardToken")
        return self._rewardToken

    @property
    async def totalAllocPoint(self) -> int:
        """Sum of the allocation points of all pools

        Returns:
            int: totalAllocPoint
        """
        if not self._totalAllocPoint:
            self._totalAllocPoint = await self.call_function_autoRpc("totalAllocPoint")
        return self._totalAllocPoint

    async def userInfo(self, pid: int, user: str) -> tuple[int, int]:
        """_summary_

        Args:
            pid (int): pool index
            user (str): user address

        Returns:
            tuple[int, int]: amount uint256, rewardDebt uint256
                    amount — how many Liquid Provider (LP) tokens the user has supplied
                    rewardDebt — the amount of SUSHI entitled to the user

        """
        return await self.call_function_autoRpc("userInfo", None, pid, user)

    # CUSTOM
    async def as_dict(self, convert_bint=False, static_mode: bool = False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
            static_mode (bool, optional): only general static fields are returned. Defaults to False.

        Returns:
            dict:
        """
        result = await super().as_dict(convert_bint=convert_bint)

        result["type"] = "gamma"

        result["token_precision"] = await self.acc_token_precision
        result["masterchef_address"] = (await self.masterchef_v2).lower()
        result["owner"] = (await self.owner).lower()
        result["pendingOwner"] = (await self.pendingOwner).lower()

        result["poolLength"] = await self.poolLength

        result["rewardPerSecond"] = await self.rewardPerSecond
        result["rewardToken"] = (await self.rewardToken).lower()

        result["totalAllocPoint"] = await self.totalAllocPoint

        if convert_bint:
            result["token_precision"] = str(self.acc_token_precision)
            result["rewardPerSecond"] = str(self.rewardPerSecond)
            result["totalAllocPoint"] = str(self.totalAllocPoint)

        # only return when static mode is off
        if not static_mode:
            pass

        return result


# Gamma rewarder registry ( masterchef)
class gamma_masterchef_v1(web3wrap):
    # https://optimistic.etherscan.io/address/0xc7846d1bc4d8bcf7c45a7c998b77ce9b3c904365#readContract

    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "masterchef_v1"
        self._abi_path = abi_path or "sources/common/abis/gamma/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

        self._sushi: str | None = None
        self._owner: str | None = None
        self._pendingOwner: str | None = None
        self._poolLength: int | None = None

    @property
    async def sushi(self) -> str:
        """The SUSHI token contract address

        Returns:
            str: token address
        """
        if not self._sushi:
            self._sushi = await self.call_function_autoRpc("SUSHI")
        return self._sushi

    async def getRewarder(self, pid: int, rid: int) -> str:
        """Retrieve rewarder address from masterchef

        Args:
            pid (int): The index of the pool
            rid (int): The index of the rewarder

        Returns:
            str: address
        """
        return await self.call_function_autoRpc("getRewarder", None, pid, rid)

    async def lpToken(self, pid: int) -> str:
        """Retrieve lp token address (hypervisor) from masterchef

        Args:
            index (int): index of the pool ( same of rewarder )

        Returns:
            str:  hypervisor address ( LP token)
        """
        return await self.call_function_autoRpc("lpToken", None, pid)

    @property
    async def owner(self) -> str:
        if not self._owner:
            self._owner = await self.call_function_autoRpc("owner")
        return self._owner

    @property
    async def pendingOwner(self) -> str:
        if not self._pendingOwner:
            self._pendingOwner = await self.call_function_autoRpc("pendingOwner")
        return self._pendingOwner

    async def pendingSushi(self, pid: int, user: str) -> int:
        """pending SUSHI reward for a given user

        Args:
            pid (int): The index of the pool
            user (str):  address

        Returns:
            int: _description_
        """
        return await self.call_function_autoRpc("pendingSushi", None, pid, user)

    async def poolInfo(self, pid: int) -> tuple[int, int, int]:
        """_summary_

        Returns:
            tuple[int,int,int]:  accSushiPerShare uint128, lastRewardTime uint64, allocPoint uint64
        """
        return await self.call_function_autoRpc("poolInfo", None, pid)

    @property
    async def poolLength(self) -> int:
        """Returns the number of MCV2 pools
        Returns:
            int:
        """
        if not self._poolLength:
            self._poolLength = await self.call_function_autoRpc("poolLength")
        return self._poolLength


class gamma_masterchef_v2(web3wrap):
    # https://polygonscan.com/address/0xcc54afcecd0d89e0b2db58f5d9e58468e7ad20dc#readContract

    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "masterchef_v2"
        self._abi_path = abi_path or "sources/common/abis/gamma/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

        self._endTimestamp: int | None = None
        self._erc20: str | None = None
        self._feeAddress: str | None = None
        self._owner: str | None = None
        self._paidOut: int | None = None
        self._poolLength: int | None = None
        self._rewardPerSecond: int | None = None
        self._startTimestamp: int | None = None
        self._totalAllocPoint: int | None = None

    async def deposited(self, pid: int, user: str) -> int:
        """_summary_

        Args:
            pid (int): _description_
            user (str): _description_

        Returns:
            int: _description_
        """
        return await self.call_function_autoRpc("deposited", None, pid, user)

    @property
    async def endTimestamp(self) -> int:
        """_summary_

        Returns:
            int: _description_
        """
        if not self._endTimestamp:
            self._endTimestamp = await self.call_function_autoRpc("endTimestamp")
        return self._endTimestamp

    @property
    async def erc20(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._erc20:
            self._erc20 = await self.call_function_autoRpc("erc20")
        return self._erc20

    @property
    async def feeAddress(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._feeAddress:
            self._feeAddress = await self.call_function_autoRpc("feeAddress")
        return self._feeAddress

    @property
    async def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._owner:
            self._owner = await self.call_function_autoRpc("owner")
        return self._owner

    @property
    async def paidOut(self) -> int:
        """_summary_

        Returns:
            int: _description_
        """
        if not self._paidOut:
            self._paidOut = await self.call_function_autoRpc("paidOut")
        return self._paidOut

    async def pending(self, pid: int, user: str) -> int:
        """_summary_

        Args:
            pid (int): pool index
            user (str): address

        Returns:
            int: _description_
        """
        return await self.call_function_autoRpc("pending", None, pid, user)

    async def poolInfo(self, pid: int) -> tuple[str, int, int, int, int]:
        """_summary_

        Args:
            pid (int): pool index

        Returns:
            tuple:
                lpToken address,
                allocPoint uint256,
                lastRewardTimestamp uint256,
                accERC20PerShare uint256,
                depositFeeBP uint16
        """
        return await self.call_function_autoRpc("poolInfo", None, pid)

    @property
    async def poolLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._poolLength:
            self._poolLength = await self.call_function_autoRpc("poolLength")
        return self._poolLength

    @property
    async def rewardPerSecond(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._rewardPerSecond:
            self._rewardPerSecond = await self.call_function_autoRpc("rewardPerSecond")
        return self._rewardPerSecond

    @property
    async def startTimestamp(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._startTimestamp:
            self._startTimestamp = await self.call_function_autoRpc("startTimestamp")
        return self._startTimestamp

    @property
    async def totalAllocPoint(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._totalAllocPoint:
            self._totalAllocPoint = await self.call_function_autoRpc("totalAllocPoint")
        return self._totalAllocPoint

    async def userInfo(self, pid: int, user: str) -> tuple[int, int]:
        """_summary_

        Args:
            pid (int): pool index
            user (str): address

        Returns:
            tuple:
                amount uint256,
                rewardDebt uint256
        """
        return await self.call_function_autoRpc("userInfo", None, pid, user)


# Gamma masterchef registry ( registry of the "rewarders registry")
class gamma_masterchef_registry(web3wrap):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "masterchef_registry_v1"
        self._abi_path = abi_path or "sources/common/abis/gamma/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

        self._owner: str | None = None
        self._counter: int | None = None

    # implement harcoded erroneous addresses to reduce web3 calls
    __blacklist_addresses = {}

    @property
    async def counter(self) -> int:
        """number of hypervisors indexed, initial being 0  and end the counter value-1

        Returns:
            int: positions of hypervisors in registry
        """
        if not self._counter:
            counter = await self.call_function_autoRpc("counter")
        return self._counter

    async def hypeByIndex(self, index: int) -> tuple[str, int]:
        """Retrieve hype address and index from registry
            When index is zero, hype address has been deleted so its no longer valid

        Args:
            index (int): index position of hype in registry

        Returns:
            tuple[str, int]: hype address and index
        """
        return await self.call_function_autoRpc("hypeByIndex", None, index)

    @property
    async def owner(self) -> str:
        if not self._owner:
            self._owner = await self.call_function_autoRpc("owner")
        return self._owner

    async def registry(self, index: int) -> str:
        return await self.call_function_autoRpc("registry")

    async def registryMap(self, address: str) -> int:
        return await self.call_function_autoRpc(
            "registryMap", None, Web3.to_checksum_address(address)
        )

    # CUSTOM FUNCTIONS

    # TODO: manage versions
    async def get_masterchef_list(self) -> list[gamma_masterchef_v1]:
        """Retrieve masterchef contracts from registry

        Returns:
           masterchefV2 contract
        """
        result = []
        total_qtty = await self.counter + 1  # index positions ini=0 end=counter
        for i in range(total_qtty):
            try:
                address, idx = await self.hypeByIndex(index=i)

                # filter blacklisted hypes
                if idx == 0 or (
                    self._network in self.__blacklist_addresses
                    and address.lower() in self.__blacklist_addresses[self._network]
                ):
                    # hypervisor is blacklisted: loop
                    continue

                result.append(
                    gamma_masterchef_v1(
                        address=address,
                        network=self._network,
                        block=await self.block,
                    )
                )

            except Exception:
                logging.getLogger(__name__).warning(
                    f" Masterchef registry returned the address {address} and may not be a masterchef contract ( at web3 chain id: {self._chain_id} )"
                )

        return result

    async def get_masterchef_addresses(self) -> list[str]:
        """Retrieve masterchef addresses from registry

        Returns:
           list of addresses
        """

        total_qtty = await self.counter + 1  # index positions ini=0 end=counter

        result = []
        for i in range(total_qtty):
            # executiuon reverted:  arbitrum and mainnet have diff ways of indexing (+1 or 0)
            with contextlib.suppress(Exception):
                address, idx = await self.hypeByIndex(index=i)

                # filter erroneous and blacklisted hypes
                if idx == 0 or (
                    self._network in self.__blacklist_addresses
                    and address.lower() in self.__blacklist_addresses[self._network]
                ):
                    # hypervisor is blacklisted: loop
                    continue

                result.append(address)

        return result


# Zyberswap rewarder
class zyberswap_masterchef_rewarder(web3wrap):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "zyberchef_rewarder"
        self._abi_path = abi_path or "sources/common/abis/zyberchef/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

        self._distributorV2: str | None = None
        self._isNative: bool | None = None
        self._owner: str | None = None
        self._rewardInfoLimit: int | None = None
        self._rewardToken: str | None = None
        self._totalAllocPoint: int | None = None

    async def _getTimeElapsed(self, _from: int, _to: int, _endTimestamp: int) -> int:
        return await self.call_function_autoRpc(
            "_getTimeElapsed", None, _from, _to, _endTimestamp
        )

    async def currentTimestamp(self, pid: int) -> int:
        return await self.call_function_autoRpc("currentTimestamp", None, pid)

    @property
    async def distributorV2(self) -> str:
        if not self._distributorV2:
            self._distributorV2 = await self.call_function_autoRpc("distributorV2")
        return self._distributorV2

    @property
    async def isNative(self) -> bool:
        if not self._isNative:
            self._isNative = await self.call_function_autoRpc("isNative")
        return self._isNative

    @property
    async def owner(self) -> str:
        if not self._owner:
            self._owner = await self.call_function_autoRpc("owner")
        return self._owner

    async def pendingTokens(self, pid: int, user: str) -> int:
        return await self.call_function_autoRpc("pendingTokens", None, pid, user)

    async def poolIds(self, input: int) -> int:
        return await self.call_function_autoRpc("poolIds", None, input)

    async def poolInfo(self, pid: int) -> tuple[int, int, int, int, int]:
        """

        Args:
            pid (int): pool index

        Returns:
            tuple[int, int, int, int, int]:
                accTokenPerShare uint256
                startTimestamp unit256
                lastRewardTimestamp uint256
                allocPoint uint256 — allocation points assigned to the pool.
                totalRewards uint256 — total rewards for the pool
        """
        return await self.call_function_autoRpc("poolInfo", None, pid)

    async def poolRewardInfo(self, input1: int, input2: int) -> tuple[int, int, int]:
        """_summary_

        Args:
            input1 (int): _description_
            input2 (int): _description_

        Returns:
            tuple[int,int,int]:  startTimestamp uint256, endTimestamp uint256, rewardPerSec uint256
        """
        return await self.call_function_autoRpc("poolRewardInfo", None, input1, input2)

    async def poolRewardsPerSec(self, pid: int) -> int:
        return await self.call_function_autoRpc("poolRewardsPerSec", None, pid)

    @property
    async def rewardInfoLimit(self) -> int:
        if not self._rewardInfoLimit:
            self._rewardInfoLimit = await self.call_function_autoRpc("rewardInfoLimit")
        return self._rewardInfoLimit

    @property
    async def rewardToken(self) -> str:
        if not self._rewardToken:
            self._rewardToken = await self.call_function_autoRpc("rewardToken")
        return self._rewardToken

    @property
    async def totalAllocPoint(self) -> int:
        """Sum of the allocation points of all pools

        Returns:
            int: totalAllocPoint
        """
        if not self._totalAllocPoint:
            self._totalAllocPoint = await self.call_function_autoRpc("totalAllocPoint")
        return self._totalAllocPoint

    async def userInfo(self, pid: int, user: str) -> tuple[int, int]:
        """_summary_

        Args:
            pid (int): pool index
            user (str): user address

        Returns:
            tuple[int, int]: amount uint256, rewardDebt uint256
                    amount — how many Liquid Provider (LP) tokens the user has supplied
                    rewardDebt — the amount of SUSHI entitled to the user

        """
        return await self.call_function_autoRpc("userInfo", None, pid, user)

    # CUSTOM
    async def as_dict(self, convert_bint=False, static_mode: bool = False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
            static_mode (bool, optional): only general static fields are returned. Defaults to False.

        Returns:
            dict:
        """
        result = await super().as_dict(convert_bint=convert_bint)

        result["type"] = "zyberswap"
        # result["token_precision"] = await self.acc_token_precision

        result["masterchef_address"] = (await self.distributorV2).lower()
        result["owner"] = (await self.owner).lower()
        # result["pendingOwner"] = ""

        # result["poolLength"] = await self.poolLength

        # result["rewardPerSecond"] = await self.rewardPerSecond

        result["rewardToken"] = (await self.rewardToken).lower()
        result["totalAllocPoint"] = await self.totalAllocPoint

        if convert_bint:
            result["totalAllocPoint"] = str(result["totalAllocPoint"])
            # result["rewardPerSecond"] = str(result["rewardPerSecond"])
            # result["token_precision"] = str(result["token_precision"])

        # only return when static mode is off
        if not static_mode:
            pass

        return result


# Zyberswap rewarder registry ( masterchef)
class zyberswap_masterchef_v1(web3wrap):
    # https://arbiscan.io/address/0x9ba666165867e916ee7ed3a3ae6c19415c2fbddd#readContract
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "zyberchef_v1"
        self._abi_path = abi_path or "sources/common/abis/zyberswap/masterchef"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

        self._maximum_deposit_fee_rate: int | None = None
        self._maximum_harvest_interval: int | None = None
        self._feeAddress: str | None = None
        self._getZyberPerSecond: int | None = None
        self._marketingAddress: str | None = None
        self._marketingPercent: int | None = None
        self._owner: str | None = None
        self._poolLength: int | None = None
        self._startTimestamp: int | None = None
        self._teamAddress: str | None = None
        self._teamPercent: int | None = None
        self._totalAllocPoint: int | None = None
        self._totalLockedUpRewards: int | None = None
        self._totalZyberInPools: int | None = None
        self._zyber: str | None = None
        self._zyberPerSecond: int | None = None

    @property
    async def maximum_deposit_fee_rate(self) -> int:
        """maximum deposit fee rate

        Returns:
            int: unit16
        """
        if not self._maximum_deposit_fee_rate:
            self._maximum_deposit_fee_rate = await self.call_function_autoRpc(
                "MAXIMUM_DEPOSIT_FEE_RATE"
            )
        return self._maximum_deposit_fee_rate

    @property
    async def maximum_harvest_interval(self) -> int:
        """maximum harvest interval

        Returns:
            int: unit256
        """
        if not self._maximum_harvest_interval:
            self._maximum_harvest_interval = await self.call_function_autoRpc(
                "MAXIMUM_HARVEST_INTERVAL"
            )

        return self._maximum_harvest_interval

    async def canHarvest(self, pid: int, user: str) -> bool:
        """can harvest

        Args:
            pid (int): pool id
            user (str): user address

        Returns:
            bool: _description_
        """
        return await self.call_function_autoRpc("canHarvest", None, pid, user)

    @property
    async def feeAddress(self) -> str:
        """fee address

        Returns:
            str: address
        """
        if not self._feeAddress:
            self._feeAddress = await self.call_function_autoRpc("feeAddress")
        return self._feeAddress

    @property
    async def getZyberPerSec(self) -> int:
        """zyber per sec

        Returns:
            int: unit256
        """
        if not self._getZyberPerSecond:
            self._getZyberPerSecond = await self.call_function_autoRpc("getZyberPerSec")
        return self._getZyberPerSecond

    @property
    async def marketingAddress(self) -> str:
        """marketing address

        Returns:
            str: address
        """
        if not self._marketingAddress:
            self._marketingAddress = await self.call_function_autoRpc(
                "marketingAddress"
            )
        return self._marketingAddress

    @property
    async def marketingPercent(self) -> int:
        """marketing percent

        Returns:
            int: unit256
        """
        if not self._marketingPercent:
            self._marketingPercent = await self.call_function_autoRpc(
                "marketingPercent"
            )
        return self._marketingPercent

    @property
    async def owner(self) -> str:
        """owner

        Returns:
            str: address
        """
        if not self._owner:
            self._owner = await self.call_function_autoRpc("owner")
        return self._owner

    async def pendingTokens(
        self, pid: int, user: str
    ) -> tuple[list[str], list[str], list[int], list[int]]:
        """pending tokens

        Args:
            pid (int): pool id
            user (str): user address

        Returns:
            tuple: addresses address[], symbols string[], decimals uint256[], amounts uint256[]
        """
        return await self.call_function_autoRpc("pendingTokens", None, pid, user)

    async def poolInfo(self, pid: int) -> tuple[str, int, int, int, int, int, int, int]:
        """pool info

        Args:
            pid (int): pool id

        Returns:
            tuple:
                lpToken address,
                allocPoint uint256,
                lastRewardTimestamp uint256,
                accZyberPerShare uint256,
                depositFeeBP uint16,
                harvestInterval uint256,
                totalLp uint256
        """
        return await self.call_function_autoRpc("poolInfo", None, pid)

    @property
    async def poolLength(self) -> int:
        """pool length

        Returns:
            int: unit256
        """
        if not self._poolLength:
            self._poolLength = await self.call_function_autoRpc("poolLength")
        return self._poolLength

    async def poolRewarders(self, pid: int) -> list[str]:
        """pool rewarders

        Args:
            pid (int): pool id

        Returns:
            list[str]: address[]
        """
        return await self.call_function_autoRpc("poolRewarders", None, pid)

    async def poolRewardsPerSec(
        self, pid: int
    ) -> tuple[list[str], list[str], list[int], list[int]]:
        """pool rewards per sec

        Args:
            pid (int): pool id

        Returns:
            tuple: addresses address[],
            symbols string[],
            decimals uint256[],
            rewardsPerSec uint256[]
        """
        return await self.call_function_autoRpc("poolRewardsPerSec", None, pid)

    async def poolTotalLp(self, pid: int) -> int:
        """pool total lp

        Args:
            pid (int): pool id

        Returns:
            int: unit256
        """
        return await self.call_function_autoRpc("poolTotalLp", None, pid)

    @property
    async def startTimestamp(self) -> int:
        """start timestamp

        Returns:
            int: unit256
        """
        if not self._startTimestamp:
            self._startTimestamp = await self.call_function_autoRpc("startTimestamp")
        return self._startTimestamp

    @property
    async def teamAddress(self) -> str:
        """team address

        Returns:
            str: address
        """
        if not self._teamAddress:
            self._teamAddress = await self.call_function_autoRpc("teamAddress")
        return self._teamAddress

    @property
    async def teamPercent(self) -> int:
        """team percent

        Returns:
            int: unit256
        """
        if not self._teamPercent:
            self._teamPercent = await self.call_function_autoRpc("teamPercent")
        return self._teamPercent

    @property
    async def totalAllocPoint(self) -> int:
        """total alloc point

        Returns:
            int: unit256
        """
        if not self._totalAllocPoint:
            self._totalAllocPoint = await self.call_function_autoRpc("totalAllocPoint")
        return self._totalAllocPoint

    @property
    async def totalLockedUpRewards(self) -> int:
        """total locked up rewards

        Returns:
            int: unit256
        """
        if not self._totalLockedUpRewards:
            self._totalLockedUpRewards = await self.call_function_autoRpc(
                "totalLockedUpRewards"
            )
        return self._totalLockedUpRewards

    @property
    async def totalZyberInPools(self) -> int:
        """total zyber in pools

        Returns:
            int: unit256
        """
        if not self._totalZyberInPools:
            self._totalZyberInPools = await self.call_function_autoRpc(
                "totalZyberInPools"
            )
        return self._totalZyberInPools

    async def userInfo(self, pid: int, user: str) -> tuple[int, int, int, int]:
        """user info

        Args:
            pid (int): pool id
            user (str): user address

        Returns:
            tuple:
                amount uint256,
                rewardDebt uint256,
                rewardLockedUp uint256,
                nextHarvestUntil uint256
        """
        return await self.call_function_autoRpc("userInfo", None, pid, user)

    @property
    async def zyber(self) -> str:
        """zyber

        Returns:
            str: address
        """
        if not self._zyber:
            self._zyber = await self.call_function_autoRpc("zyber")
        return self._zyber

    @property
    async def zyberPerSec(self) -> int:
        """zyber per sec

        Returns:
            int: unit256
        """
        if not self._zyberPerSec:
            self._zyberPerSec = await self.call_function_autoRpc("zyberPerSec")
        return self._zyberPerSec


# Thena voter ( sort of masterchef)
class thena_voter_v3(web3wrap):
    # https://bscscan.com/address/0x374cc2276b842fecd65af36d7c60a5b78373ede1#readContract
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "voterV3"
        self._abi_path = abi_path or "sources/common/abis/thena/binance"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

        self._max_vote_delay: int | None = None
        self._vote_delay: int | None = None
        self.__epochTimestamp: int | None = None
        self.__factories: list[str] | None = None
        self.__ve: str | None = None
        self._bribefactory: str | None = None
        self._factory: str | None = None
        self._factoryLength: int | None = None
        self._gaugeFactoriesLength: int | None = None
        self._gaugefactory: str | None = None
        self._isAlive: bool | None = None
        self._length: int | None = None
        self._minter: str | None = None
        self._owner: str | None = None
        self._permissionRegistry: str | None = None
        self._totalWeight: int | None = None

    @property
    async def max_vote_delay(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._max_vote_delay:
            self._max_vote_delay = await self.call_function_autoRpc("MAX_VOTE_DELAY")
        return self._max_vote_delay

    @property
    async def vote_delay(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._vote_delay:
            self._vote_delay = await self.call_function_autoRpc("VOTE_DELAY")
        return self._vote_delay

    @property
    async def _epochTimestamp(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self.__epochTimestamp:
            self.__epochTimestamp = await self.call_function_autoRpc("_epochTimestamp")
        return self.__epochTimestamp

    @property
    async def _factories(self) -> list[str]:
        """_summary_

        Returns:
            list[str]: address[]
        """
        if not self.__factories:
            self.__factories = await self.call_function_autoRpc("_factories")
        return self.__factories

    @property
    async def _ve(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self.__ve:
            self.__ve = await self.call_function_autoRpc("_ve")
        return self.__ve

    @property
    async def bribefactory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._bribefactory:
            self._bribefactory = await self.call_function_autoRpc("bribefactory")
        return self._bribefactory

    async def claimable(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return await self.call_function_autoRpc("claimable")

    async def external_bribes(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return await self.call_function_autoRpc(
            "external_bribes", None, Web3.to_checksum_address(address)
        )

    async def factories(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return await self.call_function_autoRpc("factories", None, index)

    @property
    async def factory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._factory:
            self._factory = await self.call_function_autoRpc("factory")
        return self._factory

    @property
    async def factoryLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._factoryLength:
            self._factoryLength = await self.call_function_autoRpc("factoryLength")
        return self._factoryLength

    async def gaugeFactories(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return await self.call_function_autoRpc("gaugeFactories", None, index)

    @property
    async def gaugeFactoriesLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._gaugeFactoriesLength:
            self._gaugeFactoriesLength = await self.call_function_autoRpc(
                "gaugeFactoriesLength"
            )
        return self._gaugeFactoriesLength

    @property
    async def gaugefactory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._gaugefactory:
            self._gaugefactory = await self.call_function_autoRpc("gaugefactory")
        return self._gaugefactory

    async def gauges(self, address: str) -> str:
        """_summary_

        Args:
            address (str):

        Returns:
            str: address
        """
        return await self.call_function_autoRpc(
            "gauges", None, Web3.to_checksum_address(address)
        )

    async def gaugesDistributionTimestamp(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return await self.call_function_autoRpc(
            "gaugesDistributionTimestamp", None, Web3.to_checksum_address(address)
        )

    async def internal_bribes(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return await self.call_function_autoRpc(
            "internal_bribes", None, Web3.to_checksum_address(address)
        )

    @property
    async def isAlive(self) -> bool:
        """_summary_

        Returns:
            bool: bool
        """
        if not self._isAlive:
            self._isAlive = await self.call_function_autoRpc("isAlive")
        return self._isAlive

    async def isFactory(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return await self.call_function_autoRpc(
            "isFactory", None, Web3.to_checksum_address(address)
        )

    async def isGauge(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return await self.call_function_autoRpc(
            "isGauge", None, Web3.to_checksum_address(address)
        )

    async def isGaugeFactory(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return await self.call_function_autoRpc(
            "isGaugeFactory", None, Web3.to_checksum_address(address)
        )

    async def isWhitelisted(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return await self.call_function_autoRpc(
            "isWhitelisted", None, Web3.to_checksum_address(address)
        )

    async def lastVoted(self, index: int) -> int:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            int: uint256
        """
        return await self.call_function_autoRpc("lastVoted", None, index)

    @property
    async def length(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._length:
            self._length = await self.call_function_autoRpc("length")
        return self._length

    @property
    async def minter(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._minter:
            self._minter = await self.call_function_autoRpc("minter")
        return self._minter

    @property
    async def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._owner:
            self._owner = await self.call_function_autoRpc("owner")
        return self._owner

    @property
    async def permissionRegistry(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._permissionRegistry:
            self._permissionRegistry = await self.call_function_autoRpc(
                "permissionRegistry"
            )
        return self._permissionRegistry

    async def poolForGauge(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return await self.call_function_autoRpc(
            "poolForGauge", None, Web3.to_checksum_address(address)
        )

    async def poolVote(self, input1: int, input2: int) -> str:
        """_summary_

        Args:
            input1 (int): uint256
            input2 (int): uint256

        Returns:
            str: address
        """
        return await self.call_function_autoRpc("poolVote", None, input1, input2)

    async def poolVoteLength(self, tokenId: int) -> int:
        """_summary_

        Args:
            tokenId (int): uint256

        Returns:
            int: uint256
        """
        return await self.call_function_autoRpc("poolVoteLength", None, tokenId)

    async def pools(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return await self.call_function_autoRpc("pools", None, index)

    @property
    async def totalWeight(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._totalWeight:
            self._totalWeight = await self.call_function_autoRpc("totalWeight")
        return self._totalWeight

    async def totalWeightAt(self, time: int) -> int:
        """_summary_

        Args:
            time (int): uint256

        Returns:
            int: uint256
        """
        return await self.call_function_autoRpc("totalWeightAt", None, time)

    async def usedWeights(self, index: int) -> int:
        """_summary_

        Args:
            index (int)

        Returns:
            int: uint256
        """
        return await self.call_function_autoRpc("usedWeights", None, index)

    async def votes(self, index: int, address: str) -> int:
        """_summary_

        Args:
            index (int): uint256
            address (str): address

        Returns:
            int: uint256
        """
        return await self.call_function_autoRpc(
            "votes", None, index, Web3.to_checksum_address(address)
        )

    async def weights(self, pool_address: str) -> int:
        """_summary_

        Args:
            pool_address (str): address

        Returns:
            int: uint256
        """
        return await self.call_function_autoRpc(
            "weights", None, Web3.to_checksum_address(pool_address)
        )

    async def weightsAt(self, pool_address: str, time: int) -> int:
        """_summary_

        Args:
            pool_address (str): address
            time (int): uint256

        Returns:
            int: uint256
        """
        return await self.call_function_autoRpc(
            "weightsAt", None, Web3.to_checksum_address(pool_address), time
        )


# Thena rewarder
class thena_gauge_V2(web3wrap):
    # https://bscscan.com/address/0x0C83DbCdf4a43F5F015Bf65C0761024D328F3776#readContract
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "gaugeV2_CL"
        self._abi_path = abi_path or "sources/common/abis/thena/binance"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

        self._distribution: str | None = None
        self._duration: int | None = None
        self._token: str | None = None
        self.__ve: str | None = None
        self.__periodFinish: int | None = None
        # self._periodFinish: int | None = None
        # self.__totalSupply: int | None = None
        self._emergency: bool | None = None
        self._external_bribe: str | None = None
        self._feeVault: str | None = None
        self._fees0: int | None = None
        self._fees1: int | None = None
        self._gaugeRewarder: str | None = None
        self._internal_bribe: str | None = None
        self._lastTimeRewardApplicable: int | None = None
        self._lastUpdateTime: int | None = None
        self._owner: str | None = None
        self._rewardPerDuration: int | None = None
        self._rewardPerToken: int | None = None
        self._rewardPerTokenStored: int | None = None
        self._rewardRate: int | None = None
        self._rewardToken: str | None = None
        self._rewardPid: int | None = None
        self._totalSupply: int | None = None

    @property
    async def distribution(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._distribution:
            self._distribution = await self.call_function_autoRpc("DISTRIBUTION")
        return self._distribution

    @property
    async def duration(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._duration:
            self._duration = await self.call_function_autoRpc("DURATION")
        return self._duration

    @property
    async def token(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._token:
            self._token = await self.call_function_autoRpc("TOKEN")
        return self._token

    @property
    async def _ve(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self.__ve:
            self.__ve = await self.call_function_autoRpc("_ve")
        return self.__ve

    async def _balances(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return await self.call_function_autoRpc(
            "_balances", None, Web3.to_checksum_address(address)
        )

    @property
    async def _periodFinish(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self.__periodFinish:
            self.__periodFinish = await self.call_function_autoRpc("_periodFinish")
        return self.__periodFinish

    # @property
    # async def _totalSupply(self) -> int:
    #     """_summary_

    #     Returns:
    #         int: uint256
    #     """
    #     if not self.__totalSupply:
    #         self.__totalSupply = await self.call_function_autoRpc("_totalSupply")
    #     return self.__totalSupply

    async def balanceOf(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return await self.call_function_autoRpc(
            "balanceOf", None, Web3.to_checksum_address(address)
        )

    async def earned(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return await self.call_function_autoRpc(
            "earned", None, Web3.to_checksum_address(address)
        )

    @property
    async def emergency(self) -> bool:
        """_summary_

        Returns:
            bool: bool
        """
        if not self._emergency:
            self._emergency = await self.call_function_autoRpc("emergency")
        return self._emergency

    @property
    async def external_bribe(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._external_bribe:
            self._external_bribe = await self.call_function_autoRpc("external_bribe")
        return self._external_bribe

    @property
    async def feeVault(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._feeVault:
            self._feeVault = await self.call_function_autoRpc("feeVault")
        return self._feeVault

    @property
    async def fees0(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._fees0:
            self._fees0 = await self.call_function_autoRpc("fees0")
        return self._fees0

    @property
    async def fees1(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._fees1:
            self._fees1 = await self.call_function_autoRpc("fees1")
        return self._fees1

    @property
    async def gaugeRewarder(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._gaugeRewarder:
            self._gaugeRewarder = await self.call_function_autoRpc("gaugeRewarder")
        return self._gaugeRewarder

    @property
    async def internal_bribe(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._internal_bribe:
            self._internal_bribe = await self.call_function_autoRpc("internal_bribe")
        return self._internal_bribe

    @property
    async def lastTimeRewardApplicable(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._lastTimeRewardApplicable:
            self._lastTimeRewardApplicable = await self.call_function_autoRpc(
                "lastTimeRewardApplicable"
            )
        return self._lastTimeRewardApplicable

    @property
    async def lastUpdateTime(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._lastUpdateTime:
            self._lastUpdateTime = await self.call_function_autoRpc("lastUpdateTime")
        return self._lastUpdateTime

    @property
    async def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._owner:
            self._owner = await self.call_function_autoRpc("owner")
        return self._owner

    # @property
    # async def periodFinish(self) -> int:
    #     """_summary_

    #     Returns:
    #         int: uint256
    #     """
    #     if not self._periodFinish:
    #         self._periodFinish = await self.call_function_autoRpc("periodFinish")
    #     return self._periodFinish

    @property
    async def rewardPerDuration(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._rewardPerDuration:
            self._rewardPerDuration = await self.call_function_autoRpc(
                "rewardPerDuration"
            )
        return self._rewardPerDuration

    @property
    async def rewardPerToken(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._rewardPerToken:
            self._rewardPerToken = await self.call_function_autoRpc("rewardPerToken")
        return self._rewardPerToken

    @property
    async def rewardPerTokenStored(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._rewardPerTokenStored:
            self._rewardPerTokenStored = await self.call_function_autoRpc(
                "rewardPerTokenStored"
            )
        return self._rewardPerTokenStored

    @property
    async def rewardRate(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._rewardRate:
            self._rewardRate = await self.call_function_autoRpc("rewardRate")
        return self._rewardRate

    @property
    async def rewardToken(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._rewardToken:
            self._rewardToken = await self.call_function_autoRpc("rewardToken")
        return self._rewardToken

    @property
    async def rewardPid(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._rewardPid:
            self._rewardPid = await self.call_function_autoRpc("rewardPid")
        return self._rewardPid

    async def rewards(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return await self.call_function_autoRpc(
            "rewards", None, Web3.to_checksum_address(address)
        )

    @property
    async def totalSupply(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._totalSupply:
            self._totalSupply = await self.call_function_autoRpc("totalSupply")
        return self._totalSupply

    async def userRewardPerTokenPaid(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return await self.call_function_autoRpc(
            "userRewardPerTokenPaid", None, Web3.to_checksum_address(address)
        )
