import contextlib
import logging

from decimal import Decimal
from web3 import Web3

from sources.web3.bins.w3.objects.basic import web3wrap, erc20
from sources.web3.bins.w3.objects.exchanges import (
    univ3_pool,
    algebrav3_pool,
)


class gamma_hypervisor(erc20):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "hypervisor"
        self._abi_path = abi_path or "sources/common/abis/gamma"

        self._pool: univ3_pool | None = None
        self._token0: erc20 | None = None
        self._token1: erc20 | None = None

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    # PROPERTIES
    @property
    def baseUpper(self) -> int:
        """baseUpper _summary_

        Returns:
            _type_: 0 int24
        """
        return self._contract.functions.baseUpper().call(block_identifier=self.block)

    @property
    def baseLower(self) -> int:
        """baseLower _summary_

        Returns:
            _type_: 0 int24
        """
        return self._contract.functions.baseLower().call(block_identifier=self.block)

    @property
    def currentTick(self) -> int:
        """currentTick _summary_

        Returns:
            int: -78627 int24
        """
        return self._contract.functions.currentTick().call(block_identifier=self.block)

    @property
    def deposit0Max(self) -> int:
        """deposit0Max _summary_

        Returns:
            float: 1157920892373161954234007913129639935 uint256
        """
        return self._contract.functions.deposit0Max().call(block_identifier=self.block)

    @property
    def deposit1Max(self) -> int:
        """deposit1Max _summary_

        Returns:
            int: 115792089237 uint256
        """
        return self._contract.functions.deposit1Max().call(block_identifier=self.block)

    # v1 contracts have no directDeposit
    @property
    def directDeposit(self) -> bool:
        """v1 contracts have no directDeposit function

        Returns:
            bool:
        """
        return self._contract.functions.directDeposit().call(
            block_identifier=self.block
        )

    @property
    def fee(self) -> int:
        """fee _summary_

        Returns:
            int: 10 uint8
        """
        return self._contract.functions.fee().call(block_identifier=self.block)

    # v1 contracts have no feeRecipient
    @property
    def feeRecipient(self) -> str:
        """v1 contracts have no feeRecipient function

        Returns:
            str: address
        """
        return self._contract.functions.feeRecipient().call(block_identifier=self.block)

    @property
    def getBasePosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   287141300490401993 uint128
               amount0     72329994  uint256
               amount1     565062023318300677907  uint256
               }
        """
        tmp = self._contract.functions.getBasePosition().call(
            block_identifier=self.block
        )
        return {
            "liquidity": tmp[0],
            "amount0": tmp[1],
            "amount1": tmp[2],
        }

    @property
    def getLimitPosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   287141300490401993 uint128
               amount0     72329994 uint256
               amount1     565062023318300677907 uint256
               }
        """
        tmp = self._contract.functions.getLimitPosition().call(
            block_identifier=self.block
        )
        return {
            "liquidity": tmp[0],
            "amount0": tmp[1],
            "amount1": tmp[2],
        }

    @property
    def getTotalAmounts(self) -> dict:
        """

        Returns:
           _type_: total0   2902086313 uint256
                   total1  565062023318300678136 uint256
        """
        tmp = self._contract.functions.getTotalAmounts().call(
            block_identifier=self.block
        )
        return {
            "total0": tmp[0],
            "total1": tmp[1],
        }

    @property
    def limitLower(self) -> int:
        """limitLower _summary_

        Returns:
            int: 0 int24
        """
        return self._contract.functions.limitLower().call(block_identifier=self.block)

    @property
    def limitUpper(self) -> int:
        """limitUpper _summary_

        Returns:
            int: 0 int24
        """
        return self._contract.functions.limitUpper().call(block_identifier=self.block)

    @property
    def maxTotalSupply(self) -> int:
        """maxTotalSupply _summary_

        Returns:
            int: 0 uint256
        """
        return self._contract.functions.maxTotalSupply().call(
            block_identifier=self.block
        )

    @property
    def name(self) -> str:
        return self._contract.functions.name().call(block_identifier=self.block)

    def nonces(self, owner: str):
        return self._contract.functions.nonces()(Web3.to_checksum_address(owner)).call(
            block_identifier=self.block
        )

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def pool(self) -> univ3_pool:
        if self._pool is None:
            self._pool = univ3_pool(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._pool

    @property
    def tickSpacing(self) -> int:
        """tickSpacing _summary_

        Returns:
            int: 60 int24
        """
        return self._contract.functions.tickSpacing().call(block_identifier=self.block)

    @property
    def token0(self) -> erc20:
        if self._token0 is None:
            self._token0 = erc20(
                address=self._contract.functions.token0().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token0

    @property
    def token1(self) -> erc20:
        if self._token1 is None:
            self._token1 = erc20(
                address=self._contract.functions.token1().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._token1

    @property
    def block(self) -> int:
        return self._block

    @block.setter
    def block(self, value):
        self._block = value
        self.pool.block = value
        self.token0.block = value
        self.token1.block = value

    # CUSTOM FUNCTIONS
    def get_all_events(self):
        return NotImplementedError("get_all_events not implemented for v1 contracts")
        # return [
        #     event.createFilter(fromBlock=self.block)
        #     for event in self.contract.events
        #     if issubclass(event, TransactionEvent) # only get transaction events
        # ]

    def get_qtty_depoloyed(self, inDecimal: bool = True) -> dict:
        """Retrieve the quantity of tokens currently deployed

        Returns:
           dict: {
                   "qtty_token0":0,         # quantity of token 0 deployed in dex
                   "qtty_token1":0,         # quantity of token 1 deployed in dex
                   "fees_owed_token0":0,    # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
                   "fees_owed_token1":0,    # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
                 }
        """
        # positions
        base = self.pool.get_qtty_depoloyed(
            ownerAddress=self.address,
            tickUpper=self.baseUpper,
            tickLower=self.baseLower,
            inDecimal=inDecimal,
        )
        limit = self.pool.get_qtty_depoloyed(
            ownerAddress=self.address,
            tickUpper=self.limitUpper,
            tickLower=self.limitLower,
            inDecimal=inDecimal,
        )

        # add up
        return {k: base.get(k, 0) + limit.get(k, 0) for k in set(base) & set(limit)}

    def get_fees_uncollected(self, inDecimal: bool = True) -> dict:
        """Retrieve the quantity of fees not collected nor yet owed ( but certain) to the deployed position

        Returns:
            dict: {
                    "qtty_token0":0,  # quantity of uncollected token 0
                    "qtty_token1":0,  # quantity of uncollected token 1
                }
        """
        # positions
        base = self.pool.get_fees_uncollected(
            ownerAddress=self.address,
            tickUpper=self.baseUpper,
            tickLower=self.baseLower,
            inDecimal=inDecimal,
        )
        limit = self.pool.get_fees_uncollected(
            ownerAddress=self.address,
            tickUpper=self.limitUpper,
            tickLower=self.limitLower,
            inDecimal=inDecimal,
        )

        return {k: base.get(k, 0) + limit.get(k, 0) for k in set(base) & set(limit)}

    def get_tvl(self, inDecimal=True) -> dict:
        """get total value locked of both positions
           TVL = deployed + parked + owed

        Returns:
           dict: {" tvl_token0": ,      (int or Decimal) sum of below's token 0 (total)
                   "tvl_token1": ,      (int or Decimal)
                   "deployed_token0": , (int or Decimal) quantity of token 0 LPing
                   "deployed_token1": , (int or Decimal)
                   "fees_owed_token0": ,(int or Decimal) fees owed to the position by dex
                   "fees_owed_token1": ,(int or Decimal)
                   "parked_token0": ,   (int or Decimal) quantity of token 0 parked at contract (not deployed)
                   "parked_token1": ,   (int or Decimal)
                   }
        """
        # get deployed fees as int ( force no decimals)
        deployed = self.get_qtty_depoloyed(inDecimal=False)

        result = {"parked_token0": self.pool.token0.balanceOf(self.address)}

        result["parked_token1"] = self.pool.token1.balanceOf(self.address)

        result["deployed_token0"] = deployed["qtty_token0"]
        result["deployed_token1"] = deployed["qtty_token1"]
        result["fees_owed_token0"] = deployed["fees_owed_token0"]
        result["fees_owed_token1"] = deployed["fees_owed_token1"]

        # sumup
        result["tvl_token0"] = (
            result["deployed_token0"]
            + result["fees_owed_token0"]
            + result["parked_token0"]
        )
        result["tvl_token1"] = (
            result["deployed_token1"]
            + result["fees_owed_token1"]
            + result["parked_token1"]
        )

        if inDecimal:
            # convert to decimal
            for key in result:
                if "token0" in key:
                    result[key] = Decimal(result[key]) / Decimal(
                        10**self.token0.decimals
                    )
                elif "token1" in key:
                    result[key] = Decimal(result[key]) / Decimal(
                        10**self.token1.decimals
                    )
                else:
                    raise ValueError(f"Cant convert '{key}' field to decimal")

        return result.copy()

    def as_dict(self, convert_bint=False, static_mode: bool = False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
            static_mode (bool, optional): only general static fields are returned. Defaults to False.

        Returns:
            dict:
        """
        result = super().as_dict(convert_bint=convert_bint)

        result["name"] = self.name
        result["pool"] = self.pool.as_dict(
            convert_bint=convert_bint, static_mode=static_mode
        )

        result["fee"] = self.fee

        # identify hypervisor dex
        result["dex"] = self.identify_dex_name()

        result["deposit0Max"] = (
            str(self.deposit0Max) if convert_bint else self.deposit0Max
        )

        result["deposit1Max"] = (
            str(self.deposit1Max) if convert_bint else self.deposit1Max
        )

        # result["directDeposit"] = self.directDeposit  # not working

        # only return when static mode is off
        if not static_mode:
            self._as_dict_not_static_items(convert_bint, result)
        return result

    def _as_dict_not_static_items(self, convert_bint, result):
        result["baseLower"] = str(self.baseLower) if convert_bint else self.baseLower
        result["baseUpper"] = str(self.baseUpper) if convert_bint else self.baseUpper
        result["currentTick"] = (
            str(self.currentTick) if convert_bint else self.currentTick
        )

        result["limitLower"] = str(self.limitLower) if convert_bint else self.limitLower

        result["limitUpper"] = str(self.limitUpper) if convert_bint else self.limitUpper

        # getTotalAmounts
        result["totalAmounts"] = self.getTotalAmounts
        if convert_bint:
            result["totalAmounts"]["total0"] = str(result["totalAmounts"]["total0"])
            result["totalAmounts"]["total1"] = str(result["totalAmounts"]["total1"])

        result["maxTotalSupply"] = (
            str(self.maxTotalSupply) if convert_bint else self.maxTotalSupply
        )

        # TVL
        result["tvl"] = self.get_tvl(inDecimal=(not convert_bint))
        if convert_bint:
            for k in result["tvl"].keys():
                result["tvl"][k] = str(result["tvl"][k])

        # Deployed
        result["qtty_depoloyed"] = self.get_qtty_depoloyed(inDecimal=(not convert_bint))
        if convert_bint:
            for k in result["qtty_depoloyed"].keys():
                result["qtty_depoloyed"][k] = str(result["qtty_depoloyed"][k])

        # uncollected fees
        result["fees_uncollected"] = self.get_fees_uncollected(
            inDecimal=(not convert_bint)
        )
        if convert_bint:
            for k in result["fees_uncollected"].keys():
                result["fees_uncollected"][k] = str(result["fees_uncollected"][k])

        # positions
        result["basePosition"] = self.getBasePosition
        if convert_bint:
            self._as_dict_convert_helper(result, "basePosition")
        result["limitPosition"] = self.getLimitPosition
        if convert_bint:
            self._as_dict_convert_helper(result, "limitPosition")
        result["tickSpacing"] = (
            str(self.tickSpacing) if convert_bint else self.tickSpacing
        )

    def _as_dict_convert_helper(self, result, arg1):
        result[arg1]["liquidity"] = str(result[arg1]["liquidity"])
        result[arg1]["amount0"] = str(result[arg1]["amount0"])
        result[arg1]["amount1"] = str(result[arg1]["amount1"])


class gamma_hypervisor_algebra(gamma_hypervisor):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "algebra_hypervisor"
        self._abi_path = abi_path or "sources/common/abis/gamma"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def pool(self) -> algebrav3_pool:
        if self._pool is None:
            self._pool = algebrav3_pool(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
            )
        return self._pool


class gamma_hypervisor_quickswap(gamma_hypervisor_algebra):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "algebra_hypervisor"
        self._abi_path = abi_path or "sources/common/abis/gamma"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )


class gamma_hypervisor_zyberswap(gamma_hypervisor_algebra):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "algebra_hypervisor"
        self._abi_path = abi_path or "sources/common/abis/gamma"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )


class gamma_hypervisor_thena(gamma_hypervisor_algebra):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "algebra_hypervisor"
        self._abi_path = abi_path or "sources/common/abis/gamma"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def pool(self) -> algebrav3_pool:
        if self._pool is None:
            self._pool = algebrav3_pool(
                address=self._contract.functions.pool().call(
                    block_identifier=self.block
                ),
                network=self._network,
                block=self.block,
                abi_filename="albebrav3pool_thena",
            )
        return self._pool


# registries


class gamma_hypervisor_registry(web3wrap):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "registry"
        self._abi_path = abi_path or "sources/common/abis/gamma/ethereum"

        super().__init__(
            address=address,
            network=network,
            abi_filename=self._abi_filename,
            abi_path=self._abi_path,
            block=block,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    # implement harcoded erroneous addresses to reduce web3 calls
    __blacklist_addresses = {
        "ethereum": [
            "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599".lower()
        ],  # address:index
        "polygon": [
            "0xa9782a2c9c3fb83937f14cdfac9a6d23946c9255".lower(),
            "0xfb0bc232CD11dBe804B489860c470B7f9cc80D9F".lower(),
        ],
        "optimism": ["0xc7722271281Aa6D5D027fC9B21989BE99424834f".lower()],
        "arbitrum": ["0x38f81e638f9e268e8417F2Ff76C270597fa077A0".lower()],
    }

    @property
    def counter(self) -> int:
        """number of hypervisors indexed, initial being 0  and end the counter value

        Returns:
            int: positions of hypervisors in registry
        """
        return self._contract.functions.counter().call(block_identifier=self.block)

    def hypeByIndex(self, index: int) -> tuple[str, int]:
        """Retrieve hype address and index from registry
            When index is zero, hype address has been deleted so its no longer valid

        Args:
            index (int): index position of hype in registry

        Returns:
            tuple[str, int]: hype address and index
        """
        return self._contract.functions.hypeByIndex(index).call(
            block_identifier=self.block
        )

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    def registry(self, index: int) -> str:
        return self._contract.functions.registry(index).call(
            block_identifier=self.block
        )

    def registryMap(self, address: str) -> int:
        return self._contract.functions.registryMap(
            Web3.to_checksum_address(address)
        ).call(block_identifier=self.block)

    # CUSTOM FUNCTIONS
    def get_hypervisors_generator(self) -> gamma_hypervisor:
        """Retrieve hypervisors from registry

        Returns:
           gamma_hypervisor
        """
        total_qtty = self.counter + 1  # index positions ini=0 end=counter
        for i in range(total_qtty):
            try:
                hypervisor_id, idx = self.hypeByIndex(index=i)

                # filter blacklisted hypes
                if idx == 0 or (
                    self._network in self.__blacklist_addresses
                    and hypervisor_id.lower()
                    in self.__blacklist_addresses[self._network]
                ):
                    # hypervisor is blacklisted: loop
                    continue

                # build hypervisor
                hypervisor = gamma_hypervisor(
                    address=hypervisor_id,
                    network=self._network,
                    block=self.block,
                )
                # check this is actually an hypervisor (erroneous addresses exist like "ethereum":{"0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"})
                hypervisor.getTotalAmounts  # test func

                # return correct hypervisor
                yield hypervisor
            except Exception:
                logging.getLogger(__name__).warning(
                    f" Hypervisor registry returned the address {hypervisor_id} and may not be an hypervisor ( at web3 chain id: {self._chain_id} )"
                )

    def get_hypervisors_addresses(self) -> list[str]:
        """Retrieve hypervisors all addresses from registry

        Returns:
           list of addresses
        """

        total_qtty = self.counter + 1  # index positions ini=0 end=counter

        result = []
        for i in range(total_qtty):
            # executiuon reverted:  arbitrum and mainnet have diff ways of indexing (+1 or 0)
            with contextlib.suppress(Exception):
                hypervisor_id, idx = self.hypeByIndex(index=i)

                # filter erroneous and blacklisted hypes
                if idx == 0 or (
                    self._network in self.__blacklist_addresses
                    and hypervisor_id.lower()
                    in self.__blacklist_addresses[self._network]
                ):
                    # hypervisor is blacklisted: loop
                    continue

                result.append(hypervisor_id)

        return result


# rewarders
class gamma_masterchef_rewarder(web3wrap):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
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
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def acc_token_precision(self) -> int:
        return self._contract.functions.ACC_TOKEN_PRECISION().call(
            block_identifier=self.block
        )

    @property
    def masterchef_v2(self) -> str:
        return self._contract.functions.MASTERCHEF_V2().call(
            block_identifier=self.block
        )

    @property
    def funder(self) -> str:
        return self._contract.functions.funder().call(block_identifier=self.block)

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def pendingOwner(self) -> str:
        return self._contract.functions.pendingOwner().call(block_identifier=self.block)

    def pendingToken(self, pid: int, user: str) -> int:
        return self._contract.functions.pendingToken(pid, user).call(
            block_identifier=self.block
        )

    def pendingTokens(self, pid: int, user: str, input: int) -> tuple[list, list]:
        # rewardTokens address[], rewardAmounts uint256[]
        return self._contract.functions.pendingTokens(pid, user, input).call(
            block_identifier=self.block
        )

    def poolIds(self, input: int) -> int:
        return self._contract.functions.poolIds(input).call(block_identifier=self.block)

    def poolInfo(self, input: int) -> tuple[int, int, int]:
        """_summary_

        Args:
            input (int): _description_

        Returns:
            tuple[int, int, int]:  accSushiPerShare uint128, lastRewardTime uint64, allocPoint uint64
                accSushiPerShare — accumulated SUSHI per share, times 1e12.
                lastRewardBlock — number of block, when the reward in the pool was the last time calculated
                allocPoint — allocation points assigned to the pool. SUSHI to distribute per block per pool = SUSHI per block * pool.allocPoint / totalAllocPoint
        """
        return self._contract.functions.poolInfo(input).call(
            block_identifier=self.block
        )

    @property
    def poolLength(self) -> int:
        return self._contract.functions.poolLength().call(block_identifier=self.block)

    @property
    def rewardPerSecond(self) -> int:
        return self._contract.functions.rewardPerSecond().call(
            block_identifier=self.block
        )

    @property
    def rewardToken(self) -> str:
        return self._contract.functions.rewardToken().call(block_identifier=self.block)

    @property
    def totalAllocPoint(self) -> int:
        """Sum of the allocation points of all pools

        Returns:
            int: totalAllocPoint
        """
        return self._contract.functions.totalAllocPoint().call(
            block_identifier=self.block
        )

    def userInfo(self, pid: int, user: str) -> tuple[int, int]:
        """_summary_

        Args:
            pid (int): pool index
            user (str): user address

        Returns:
            tuple[int, int]: amount uint256, rewardDebt uint256
                    amount — how many Liquid Provider (LP) tokens the user has supplied
                    rewardDebt — the amount of SUSHI entitled to the user

        """
        return self._contract.functions.userInfo(pid, user).call(
            block_identifier=self.block
        )

    # CUSTOM
    def as_dict(self, convert_bint=False, static_mode: bool = False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
            static_mode (bool, optional): only general static fields are returned. Defaults to False.

        Returns:
            dict:
        """
        result = super().as_dict(convert_bint=convert_bint)

        result["type"] = "gamma"

        result["token_precision"] = (
            str(self.acc_token_precision) if convert_bint else self.acc_token_precision
        )
        result["masterchef_address"] = (self.masterchef_v2).lower()
        result["owner"] = (self.owner).lower()
        result["pendingOwner"] = (self.pendingOwner).lower()

        result["poolLength"] = self.poolLength

        result["rewardPerSecond"] = (
            str(self.rewardPerSecond) if convert_bint else self.rewardPerSecond
        )
        result["rewardToken"] = (self.rewardToken).lower()

        result["totalAllocPoint"] = (
            str(self.totalAllocPoint) if convert_bint else self.totalAllocPoint
        )

        # only return when static mode is off
        if not static_mode:
            pass

        return result


class zyberswap_masterchef_rewarder(web3wrap):
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
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
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    def _getTimeElapsed(self, _from: int, _to: int, _endTimestamp: int) -> int:
        return self._contract.functions._getTimeElapsed(_from, _to, _endTimestamp).call(
            block_identifier=self.block
        )

    def currentTimestamp(self, pid: int) -> int:
        return self._contract.functions._getTimeElapsed(pid).call(
            block_identifier=self.block
        )

    @property
    def distributorV2(self) -> str:
        return self._contract.functions.distributorV2().call(
            block_identifier=self.block
        )

    @property
    def isNative(self) -> bool:
        return self._contract.functions.isNative().call(block_identifier=self.block)

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    def pendingTokens(self, pid: int, user: str) -> int:
        return self._contract.functions.pendingTokens(pid, user).call(
            block_identifier=self.block
        )

    def poolIds(self, input: int) -> int:
        return self._contract.functions.poolIds(input).call(block_identifier=self.block)

    def poolInfo(self, pid: int) -> tuple[int, int, int, int, int]:
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
        return self._contract.functions.poolInfo(pid).call(block_identifier=self.block)

    def poolRewardInfo(self, input1: int, input2: int) -> tuple[int, int, int]:
        """_summary_

        Args:
            input1 (int): _description_
            input2 (int): _description_

        Returns:
            tuple[int,int,int]:  startTimestamp uint256, endTimestamp uint256, rewardPerSec uint256
        """
        return self._contract.functions.poolRewardInfo(input1, input2).call(
            block_identifier=self.block
        )

    def poolRewardsPerSec(self, pid: int) -> int:
        return self._contract.functions.poolRewardsPerSec(pid).call(
            block_identifier=self.block
        )

    @property
    def rewardInfoLimit(self) -> int:
        return self._contract.functions.rewardInfoLimit().call(
            block_identifier=self.block
        )

    @property
    def rewardToken(self) -> str:
        return self._contract.functions.rewardToken().call(block_identifier=self.block)

    @property
    def totalAllocPoint(self) -> int:
        """Sum of the allocation points of all pools

        Returns:
            int: totalAllocPoint
        """
        return self._contract.functions.totalAllocPoint().call(
            block_identifier=self.block
        )

    def userInfo(self, pid: int, user: str) -> tuple[int, int]:
        """_summary_

        Args:
            pid (int): pool index
            user (str): user address

        Returns:
            tuple[int, int]: amount uint256, rewardDebt uint256
                    amount — how many Liquid Provider (LP) tokens the user has supplied
                    rewardDebt — the amount of SUSHI entitled to the user

        """
        return self._contract.functions.userInfo(pid, user).call(
            block_identifier=self.block
        )

    # CUSTOM
    def as_dict(self, convert_bint=False, static_mode: bool = False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
            static_mode (bool, optional): only general static fields are returned. Defaults to False.

        Returns:
            dict:
        """
        result = super().as_dict(convert_bint=convert_bint)

        result["type"] = "zyberswap"
        # result["token_precision"] = (
        #     str(self.acc_token_precision) if convert_bint else self.acc_token_precision
        # )
        result["masterchef_address"] = (self.distributorV2).lower()
        result["owner"] = (self.owner).lower()
        # result["pendingOwner"] = ""

        # result["poolLength"] = self.poolLength

        # result["rewardPerSecond"] = (
        #     str(self.rewardPerSecond) if convert_bint else self.rewardPerSecond
        # )
        result["rewardToken"] = (self.rewardToken).lower()

        result["totalAllocPoint"] = (
            str(self.totalAllocPoint) if convert_bint else self.totalAllocPoint
        )

        # only return when static mode is off
        if not static_mode:
            pass

        return result


# rewarder registry
class gamma_masterchef_v1(web3wrap):
    # https://optimistic.etherscan.io/address/0xc7846d1bc4d8bcf7c45a7c998b77ce9b3c904365#readContract

    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
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
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def sushi(self) -> str:
        """The SUSHI token contract address

        Returns:
            str: token address
        """
        return self._contract.functions.SUSHI().call(block_identifier=self.block)

    def getRewarder(self, pid: int, rid: int) -> str:
        """Retrieve rewarder address from masterchef

        Args:
            pid (int): The index of the pool
            rid (int): The index of the rewarder

        Returns:
            str: address
        """
        return self._contract.functions.getRewarder(pid, rid).call(
            block_identifier=self.block
        )

    def lpToken(self, pid: int) -> str:
        """Retrieve lp token address (hypervisor) from masterchef

        Args:
            index (int): index of the pool ( same of rewarder )

        Returns:
            str:  hypervisor address ( LP token)
        """
        return self._contract.functions.lpToken(pid).call(block_identifier=self.block)

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def pendingOwner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def pendingSushi(self, pid: int, user: str) -> int:
        """pending SUSHI reward for a given user

        Args:
            pid (int): The index of the pool
            user (str):  address

        Returns:
            int: _description_
        """
        return self._contract.functions.pendingSushi(pid, user).call(
            block_identifier=self.block
        )

    def poolInfo(
        self,
    ) -> tuple[int, int, int]:
        """_summary_

        Returns:
            tuple[int,int,int]:  accSushiPerShare uint128, lastRewardTime uint64, allocPoint uint64
        """
        return self._contract.functions.poolInfo().call(block_identifier=self.block)

    @property
    def poolLength(self) -> int:
        """Returns the number of MCV2 pools
        Returns:
            int:
        """
        return self._contract.functions.poolLength().call(block_identifier=self.block)


class gamma_masterchef_v2(web3wrap):
    # https://polygonscan.com/address/0xcc54afcecd0d89e0b2db58f5d9e58468e7ad20dc#readContract

    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
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
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    def deposited(self, pid: int, user: str) -> int:
        """_summary_

        Args:
            pid (int): _description_
            user (str): _description_

        Returns:
            int: _description_
        """
        return self._contract.functions.deposited(pid, user).call(
            block_identifier=self.block
        )

    @property
    def endTimestamp(self) -> int:
        """_summary_

        Returns:
            int: _description_
        """
        return self._contract.functions.endTimestamp().call(block_identifier=self.block)

    @property
    def erc20(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.erc20().call(block_identifier=self.block)

    @property
    def feeAddress(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.feeAddress().call(block_identifier=self.block)

    @property
    def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def paidOut(self) -> int:
        """_summary_

        Returns:
            int: _description_
        """
        return self._contract.functions.paidOut().call(block_identifier=self.block)

    def pending(self, pid: int, user: str) -> int:
        """_summary_

        Args:
            pid (int): pool index
            user (str): address

        Returns:
            int: _description_
        """
        return self._contract.functions.pending(pid, user).call(
            block_identifier=self.block
        )

    def poolInfo(self, pid: int) -> tuple[str, int, int, int, int]:
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
        return self._contract.functions.poolInfo(pid).call(block_identifier=self.block)

    @property
    def poolLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.poolLength().call(block_identifier=self.block)

    @property
    def rewardPerSecond(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.rewardPerSecond().call(
            block_identifier=self.block
        )

    @property
    def startTimestamp(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.startTimestamp().call(
            block_identifier=self.block
        )

    @property
    def totalAllocPoint(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.totalAllocPoint().call(
            block_identifier=self.block
        )

    def userInfo(self, pid: int, user: str) -> tuple[int, int]:
        """_summary_

        Args:
            pid (int): pool index
            user (str): address

        Returns:
            tuple:
                amount uint256,
                rewardDebt uint256
        """
        return self._contract.functions.userInfo(pid, user).call(
            block_identifier=self.block
        )


class zyberswap_masterchef_v1(web3wrap):
    # https://arbiscan.io/address/0x9ba666165867e916ee7ed3a3ae6c19415c2fbddd#readContract
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
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
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def maximum_deposit_fee_rate(self) -> int:
        """maximum deposit fee rate

        Returns:
            int: unit16
        """
        return self._contract.functions.MAXIMUM_DEPOSIT_FEE_RATE().call(
            block_identifier=self.block
        )

    @property
    def maximum_harvest_interval(self) -> int:
        """maximum harvest interval

        Returns:
            int: unit256
        """
        return self._contract.functions.MAXIMUM_HARVEST_INTERVAL().call(
            block_identifier=self.block
        )

    def canHarvest(self, pid: int, user: str) -> bool:
        """can harvest

        Args:
            pid (int): pool id
            user (str): user address

        Returns:
            bool: _description_
        """
        return self._contract.functions.canHarvest(pid, user).call(
            block_identifier=self.block
        )

    @property
    def feeAddress(self) -> str:
        """fee address

        Returns:
            str: address
        """
        return self._contract.functions.feeAddress().call(block_identifier=self.block)

    @property
    def getZyberPerSec(self) -> int:
        """zyber per sec

        Returns:
            int: unit256
        """
        return self._contract.functions.getZyberPerSec().call(
            block_identifier=self.block
        )

    @property
    def marketingAddress(self) -> str:
        """marketing address

        Returns:
            str: address
        """
        return self._contract.functions.marketingAddress().call(
            block_identifier=self.block
        )

    @property
    def marketingPercent(self) -> int:
        """marketing percent

        Returns:
            int: unit256
        """
        return self._contract.functions.marketingPercent().call(
            block_identifier=self.block
        )

    @property
    def owner(self) -> str:
        """owner

        Returns:
            str: address
        """
        return self._contract.functions.owner().call(block_identifier=self.block)

    def pendingTokens(
        self, pid: int, user: str
    ) -> tuple[list[str], list[str], list[int], list[int]]:
        """pending tokens

        Args:
            pid (int): pool id
            user (str): user address

        Returns:
            tuple: addresses address[], symbols string[], decimals uint256[], amounts uint256[]
        """
        return self._contract.functions.pendingTokens(pid, user).call(
            block_identifier=self.block
        )

    def poolInfo(self, pid: int) -> tuple[str, int, int, int, int, int, int, int]:
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
        return self._contract.functions.poolInfo(pid).call(block_identifier=self.block)

    @property
    def poolLength(self) -> int:
        """pool length

        Returns:
            int: unit256
        """
        return self._contract.functions.poolLength().call(block_identifier=self.block)

    def poolRewarders(self, pid: int) -> list[str]:
        """pool rewarders

        Args:
            pid (int): pool id

        Returns:
            list[str]: address[]
        """
        return self._contract.functions.poolRewarders(pid).call(
            block_identifier=self.block
        )

    def poolRewardsPerSec(
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
        return self._contract.functions.poolRewardsPerSec(pid).call(
            block_identifier=self.block
        )

    def poolTotalLp(self, pid: int) -> int:
        """pool total lp

        Args:
            pid (int): pool id

        Returns:
            int: unit256
        """
        return self._contract.functions.poolTotalLp(pid).call(
            block_identifier=self.block
        )

    @property
    def startTimestamp(self) -> int:
        """start timestamp

        Returns:
            int: unit256
        """
        return self._contract.functions.startTimestamp().call(
            block_identifier=self.block
        )

    @property
    def teamAddress(self) -> str:
        """team address

        Returns:
            str: address
        """
        return self._contract.functions.teamAddress().call(block_identifier=self.block)

    @property
    def teamPercent(self) -> int:
        """team percent

        Returns:
            int: unit256
        """
        return self._contract.functions.teamPercent().call(block_identifier=self.block)

    @property
    def totalAllocPoint(self) -> int:
        """total alloc point

        Returns:
            int: unit256
        """
        return self._contract.functions.totalAllocPoint().call(
            block_identifier=self.block
        )

    @property
    def totalLockedUpRewards(self) -> int:
        """total locked up rewards

        Returns:
            int: unit256
        """
        return self._contract.functions.totalLockedUpRewards().call(
            block_identifier=self.block
        )

    @property
    def totalZyberInPools(self) -> int:
        """total zyber in pools

        Returns:
            int: unit256
        """
        return self._contract.functions.totalZyberInPools().call(
            block_identifier=self.block
        )

    def userInfo(self, pid: int, user: str) -> tuple[int, int, int, int]:
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
        return self._contract.functions.userInfo(pid, user).call(
            block_identifier=self.block
        )

    @property
    def zyber(self) -> str:
        """zyber

        Returns:
            str: address
        """
        return self._contract.functions.zyber().call(block_identifier=self.block)

    @property
    def zyberPerSec(self) -> int:
        """zyber per sec

        Returns:
            int: unit256
        """
        return self._contract.functions.zyberPerSec().call(block_identifier=self.block)


# masterchef registry ( registry of the "rewarders registry")
class gamma_masterchef_registry(web3wrap):
    # SETUP
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
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
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    # implement harcoded erroneous addresses to reduce web3 calls
    __blacklist_addresses = {}

    @property
    def counter(self) -> int:
        """number of hypervisors indexed, initial being 0  and end the counter value-1

        Returns:
            int: positions of hypervisors in registry
        """
        return self._contract.functions.counter().call(block_identifier=self.block)

    def hypeByIndex(self, index: int) -> tuple[str, int]:
        """Retrieve hype address and index from registry
            When index is zero, hype address has been deleted so its no longer valid

        Args:
            index (int): index position of hype in registry

        Returns:
            tuple[str, int]: hype address and index
        """
        return self._contract.functions.hypeByIndex(index).call(
            block_identifier=self.block
        )

    @property
    def owner(self) -> str:
        return self._contract.functions.owner().call(block_identifier=self.block)

    def registry(self, index: int) -> str:
        return self._contract.functions.registry(index).call(
            block_identifier=self.block
        )

    def registryMap(self, address: str) -> int:
        return self._contract.functions.registryMap(
            Web3.to_checksum_address(address)
        ).call(block_identifier=self.block)

    # CUSTOM FUNCTIONS

    # TODO: manage versions
    def get_masterchef_generator(self) -> gamma_masterchef_v1:
        """Retrieve masterchef contracts from registry

        Returns:
           masterchefV2 contract
        """
        total_qtty = self.counter + 1  # index positions ini=0 end=counter
        for i in range(total_qtty):
            try:
                address, idx = self.hypeByIndex(index=i)

                # filter blacklisted hypes
                if idx == 0 or (
                    self._network in self.__blacklist_addresses
                    and address.lower() in self.__blacklist_addresses[self._network]
                ):
                    # hypervisor is blacklisted: loop
                    continue

                yield gamma_masterchef_v1(
                    address=address,
                    network=self._network,
                    block=self.block,
                )

            except Exception:
                logging.getLogger(__name__).warning(
                    f" Masterchef registry returned the address {address} and may not be a masterchef contract ( at web3 chain id: {self._chain_id} )"
                )

    def get_masterchef_addresses(self) -> list[str]:
        """Retrieve masterchef addresses from registry

        Returns:
           list of addresses
        """

        total_qtty = self.counter + 1  # index positions ini=0 end=counter

        result = []
        for i in range(total_qtty):
            # executiuon reverted:  arbitrum and mainnet have diff ways of indexing (+1 or 0)
            with contextlib.suppress(Exception):
                address, idx = self.hypeByIndex(index=i)

                # filter erroneous and blacklisted hypes
                if idx == 0 or (
                    self._network in self.__blacklist_addresses
                    and address.lower() in self.__blacklist_addresses[self._network]
                ):
                    # hypervisor is blacklisted: loop
                    continue

                result.append(address)

        return result


# Special


class thena_voter_v3(web3wrap):
    # https://bscscan.com/address/0x374cc2276b842fecd65af36d7c60a5b78373ede1#readContract
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
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
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def max_vote_delay(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.MAX_VOTE_DELAY().call(
            block_identifier=self.block
        )

    @property
    def vote_delay(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.VOTE_DELAY().call(block_identifier=self.block)

    @property
    def _epochTimestamp(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions._epochTimestamp().call(
            block_identifier=self.block
        )

    @property
    def _factories(self) -> list[str]:
        """_summary_

        Returns:
            list[str]: address[]
        """
        return self._contract.functions._factories().call(block_identifier=self.block)

    @property
    def _ve(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions._ve().call(block_identifier=self.block)

    @property
    def bribefactory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.bribefactory().call(block_identifier=self.block)

    def claimable(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.claimable(address).call(
            block_identifier=self.block
        )

    def external_bribes(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return self._contract.functions.external_bribes(address).call(
            block_identifier=self.block
        )

    def factories(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return self._contract.functions.factories(index).call(
            block_identifier=self.block
        )

    @property
    def factory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.factory().call(block_identifier=self.block)

    @property
    def factoryLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.factoryLength().call(
            block_identifier=self.block
        )

    def gaugeFactories(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return self._contract.functions.gaugeFactories(index).call(
            block_identifier=self.block
        )

    @property
    def gaugeFactoriesLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.gaugeFactoriesLength().call(
            block_identifier=self.block
        )

    @property
    def gaugefactory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.gaugefactory().call(block_identifier=self.block)

    def gauges(self, address: str) -> str:
        """_summary_

        Args:
            address (str):

        Returns:
            str: address
        """
        return self._contract.functions.gauges(address).call(
            block_identifier=self.block
        )

    def gaugesDistributionTimestamp(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.gaugesDistributionTimestamp(address).call(
            block_identifier=self.block
        )

    def internal_bribes(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return self._contract.functions.internal_bribes(address).call(
            block_identifier=self.block
        )

    @property
    def isAlive(self) -> bool:
        """_summary_

        Returns:
            bool: bool
        """
        return self._contract.functions.isAlive().call(block_identifier=self.block)

    def isFactory(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return self._contract.functions.isFactory(address).call(
            block_identifier=self.block
        )

    def isGauge(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return self._contract.functions.isGauge(address).call(
            block_identifier=self.block
        )

    def isGaugeFactory(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return self._contract.functions.isGaugeFactory(address).call(
            block_identifier=self.block
        )

    def isWhitelisted(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return self._contract.functions.isWhitelisted(address).call(
            block_identifier=self.block
        )

    def lastVoted(self, index: int) -> int:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            int: uint256
        """
        return self._contract.functions.lastVoted(index).call(
            block_identifier=self.block
        )

    @property
    def length(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.length().call(block_identifier=self.block)

    @property
    def minter(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.minter().call(block_identifier=self.block)

    @property
    def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def permissionRegistry(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.permissionRegistry().call(
            block_identifier=self.block
        )

    def poolForGauge(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return self._contract.functions.poolForGauge(address).call(
            block_identifier=self.block
        )

    def poolVote(self, input1: int, input2: int) -> str:
        """_summary_

        Args:
            input1 (int): uint256
            input2 (int): uint256

        Returns:
            str: address
        """
        return self._contract.functions.poolVote(input1, input2).call(
            block_identifier=self.block
        )

    def poolVoteLength(self, tokenId: int) -> int:
        """_summary_

        Args:
            tokenId (int): uint256

        Returns:
            int: uint256
        """
        return self._contract.functions.poolVoteLength(tokenId).call(
            block_identifier=self.block
        )

    def pools(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return self._contract.functions.pools(index).call(block_identifier=self.block)

    @property
    def totalWeight(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.totalWeight().call(block_identifier=self.block)

    def totalWeightAt(self, time: int) -> int:
        """_summary_

        Args:
            time (int): uint256

        Returns:
            int: uint256
        """
        return self._contract.functions.totalWeightAt(time).call(
            block_identifier=self.block
        )

    def usedWeights(self, index: int) -> int:
        """_summary_

        Args:
            index (int)

        Returns:
            int: uint256
        """
        return self._contract.functions.usedWeights(index).call(
            block_identifier=self.block
        )

    def votes(self, index: int, address: str) -> int:
        """_summary_

        Args:
            index (int): uint256
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.votes(index, address).call(
            block_identifier=self.block
        )

    def weights(self, pool_address: str) -> int:
        """_summary_

        Args:
            pool_address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.weights(pool_address).call(
            block_identifier=self.block
        )

    def weightsAt(self, pool_address: str, time: int) -> int:
        """_summary_

        Args:
            pool_address (str): address
            time (int): uint256

        Returns:
            int: uint256
        """
        return self._contract.functions.weightsAt(pool_address, time).call(
            block_identifier=self.block
        )


class thena_gauge_V2(web3wrap):
    # https://bscscan.com/address/0x0C83DbCdf4a43F5F015Bf65C0761024D328F3776#readContract
    def __init__(
        self,
        address: str,
        network: str,
        abi_filename: str = "",
        abi_path: str = "",
        block: int = 0,
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
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    @property
    def distribution(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.DISTRIBUTION().call(block_identifier=self.block)

    @property
    def duration(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.DURATION().call(block_identifier=self.block)

    @property
    def token(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.TOKEN().call(block_identifier=self.block)

    @property
    def _ve(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions._VE().call(block_identifier=self.block)

    def _balances(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions._balances(address).call(
            block_identifier=self.block
        )

    @property
    def _periodFinish(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions._periodFinish().call(
            block_identifier=self.block
        )

    @property
    def _totalSupply(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions._totalSupply().call(block_identifier=self.block)

    def balanceOf(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.balanceOf(address).call(
            block_identifier=self.block
        )

    def earned(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.earned(address).call(
            block_identifier=self.block
        )

    @property
    def emergency(self) -> bool:
        """_summary_

        Returns:
            bool: bool
        """
        return self._contract.functions.emergency().call(block_identifier=self.block)

    @property
    def external_bribe(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.external_bribe().call(
            block_identifier=self.block
        )

    @property
    def feeVault(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.feeVault().call(block_identifier=self.block)

    @property
    def fees0(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.fees0().call(block_identifier=self.block)

    @property
    def fees1(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.fees1().call(block_identifier=self.block)

    @property
    def gaugeRewarder(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.gaugeRewarder().call(
            block_identifier=self.block
        )

    @property
    def internal_bribe(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.internal_bribe().call(
            block_identifier=self.block
        )

    @property
    def lastTimeRewardApplicable(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.lastTimeRewardApplicable().call(
            block_identifier=self.block
        )

    @property
    def lastUpdateTime(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.lastUpdateTime().call(
            block_identifier=self.block
        )

    @property
    def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.owner().call(block_identifier=self.block)

    @property
    def periodFinish(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.periodFinish().call(block_identifier=self.block)

    @property
    def rewardPerDuration(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.rewardPerDuration().call(
            block_identifier=self.block
        )

    @property
    def rewardPerToken(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.rewardPerToken().call(
            block_identifier=self.block
        )

    @property
    def rewardPerTokenStored(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.rewardPerTokenStored().call(
            block_identifier=self.block
        )

    @property
    def rewardRate(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.rewardRate().call(block_identifier=self.block)

    @property
    def rewardToken(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        return self._contract.functions.rewardToken().call(block_identifier=self.block)

    @property
    def rewardPid(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.rewardPid().call(block_identifier=self.block)

    def rewards(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.rewards(address).call(
            block_identifier=self.block
        )

    @property
    def totalSupply(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        return self._contract.functions.totalSupply().call(block_identifier=self.block)

    def userRewardPerTokenPaid(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return self._contract.functions.userRewardPerTokenPaid(address).call(
            block_identifier=self.block
        )
