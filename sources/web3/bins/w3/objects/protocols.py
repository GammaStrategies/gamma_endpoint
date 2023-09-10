import asyncio
import contextlib
import logging

from decimal import Decimal
from web3 import Web3


from sources.web3.bins.w3.objects.basic import web3wrap, erc20
from sources.web3.bins.w3.objects.exchanges import (
    ramses_pool,
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
        timestamp: int = 0,
        custom_web3: Web3 | None = None,
        custom_web3Url: str | None = None,
    ):
        self._abi_filename = abi_filename or "hypervisor"
        self._abi_path = abi_path or "sources/common/abis/gamma"

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

        # initializers

        self._symbol: str | None = None
        self._baseUpper: int | None = None
        self._baseLower: int | None = None
        self._limitUpper: int | None = None
        self._limitLower: int | None = None
        self._currentTick: int | None = None
        self._deposit0Max: int | None = None
        self._deposit1Max: int | None = None
        self._directDeposit: bool | None = None
        self._fee: int | None = None
        self._feeRecipient: str | None = None
        self._getBasePosition: dict | None = None
        self._getLimitPosition: dict | None = None
        self._getTotalAmounts: dict | None = None
        self._limitLower: int | None = None
        self._limitUpper: int | None = None
        self._maxTotalSupply: int | None = None
        self._name: str | None = None
        self._owner: str | None = None
        self._pool: univ3_pool | None = None
        self._tickSpacing: int | None = None
        self._token0: erc20 | None = None
        self._token1: erc20 | None = None

    async def init_baseUpper(self):
        """baseUpper _summary_

        Returns:
            _type_: 0 int24
        """
        self._baseUpper = await self.call_function_autoRpc("baseUpper")

    async def init_baseLower(self):
        """baseLower _summary_

        Returns:
            _type_: 0 int24
        """
        self._baseLower = await self.call_function_autoRpc("baseLower")

    async def init_currentTick(self):
        """currentTick _summary_

        Returns:
            int: -78627 int24
        """
        self._currentTick = await self.call_function_autoRpc("currentTick")

    async def init_deposit0Max(self):
        """deposit0Max _summary_

        Returns:
            float: 1157920892373161954234007913129639935 uint256
        """
        self._deposit0Max = await self.call_function_autoRpc("deposit0Max")

    async def init_deposit1Max(self):
        """deposit1Max _summary_

        Returns:
            int: 115792089237 uint256
        """
        self._deposit1Max = await self.call_function_autoRpc("deposit1Max")

    async def init_directDeposit(self):
        """v1 contracts have no directDeposit function

        Returns:
            bool:
        """
        self._directDeposit = await self.call_function_autoRpc("directDeposit")

    async def init_fee(self):
        """fee _summary_

        Returns:
            int: 10 uint8
        """
        self._fee = await self.call_function_autoRpc("fee")

    async def init_feeRecipient(self):
        """v1 contracts have no feeRecipient function

        Returns:
            str: address
        """
        try:
            self._feeRecipient = await self.call_function_autoRpc("feeRecipient")
        except Exception:
            # v1 contracts have no feeRecipient function
            self._feeRecipient = None

    async def init_getBasePosition(self):
        """
        Returns:
           dict:   {
               liquidity   287141300490401993 uint128
               amount0     72329994  uint256
               amount1     565062023318300677907  uint256
               }
        """
        tmp = await self.call_function_autoRpc("getBasePosition")
        self._getBasePosition = {
            "liquidity": tmp[0],
            "amount0": tmp[1],
            "amount1": tmp[2],
        }

    async def init_getLimitPosition(self):
        """
        Returns:
           dict:   {
               liquidity   287141300490401993 uint128
               amount0     72329994 uint256
               amount1     565062023318300677907 uint256
               }
        """
        tmp = await self.call_function_autoRpc("getLimitPosition")
        self._getLimitPosition = {
            "liquidity": tmp[0],
            "amount0": tmp[1],
            "amount1": tmp[2],
        }

    async def init_getTotalAmounts(self):
        """

        Returns:
           _type_: total0   2902086313 uint256
                   total1  565062023318300678136 uint256
        """
        tmp = await self.call_function_autoRpc("getTotalAmounts")
        self._getTotalAmounts = {
            "total0": tmp[0],
            "total1": tmp[1],
        }

    async def init_limitLower(self):
        """limitLower _summary_

        Returns:
            int: 0 int24
        """
        self._limitLower = await self.call_function_autoRpc("limitLower")

    async def init_limitUpper(self):
        """limitUpper _summary_

        Returns:
            int: 0 int24
        """
        self._limitUpper = await self.call_function_autoRpc("limitUpper")

    async def init_maxTotalSupply(self):
        """maxTotalSupply _summary_

        Returns:
            int: 0 uint256
        """
        self._maxTotalSupply = await self.call_function_autoRpc("maxTotalSupply")

    async def init_name(self):
        self._name = await self.call_function_autoRpc("name")

    async def init_owner(self):
        self._owner = await self.call_function_autoRpc("owner")

    async def init_pool(self):
        self._pool_address = await self.call_function_autoRpc("pool")
        self._pool = univ3_pool(
            address=self._pool_address,
            network=self._network,
            block=await self.block,
            timestamp=await self.timestamp,
            custom_web3Url=self.w3.provider.endpoint_uri,
        )

    async def init_tickSpacing(self):
        """tickSpacing _summary_

        Returns:
            int: 60 int24
        """
        self._tickSpacing = await self.call_function_autoRpc("tickSpacing")

    async def init_token0(self):
        self._token0_address = await self.call_function_autoRpc("token0")
        self._token0 = erc20(
            address=self._token0_address,
            network=self._network,
            block=await self.block,
            timestamp=await self.timestamp,
            custom_web3Url=self.w3.provider.endpoint_uri,
        )

    async def init_token1(self):
        self._token1_address = await self.call_function_autoRpc("token1")
        self._token1 = erc20(
            address=self._token1_address,
            network=self._network,
            block=await self.block,
            timestamp=await self.timestamp,
            custom_web3Url=self.w3.provider.endpoint_uri,
        )

    # PROPERTIES
    @property
    async def baseUpper(self) -> int:
        """baseUpper _summary_

        Returns:
            _type_: 0 int24
        """
        if not self._baseUpper:
            await self.init_baseUpper()
        return self._baseUpper

    @property
    async def baseLower(self) -> int:
        """baseLower _summary_

        Returns:
            _type_: 0 int24
        """
        if not self._baseLower:
            await self.init_baseLower()
        return self._baseLower

    @property
    async def currentTick(self) -> int:
        """currentTick _summary_

        Returns:
            int: -78627 int24
        """
        if not self._currentTick:
            await self.init_currentTick()
        return self._currentTick

    @property
    async def deposit0Max(self) -> int:
        """deposit0Max _summary_

        Returns:
            float: 1157920892373161954234007913129639935 uint256
        """
        if not self._deposit0Max:
            await self.init_deposit0Max()
        return self._deposit0Max

    @property
    async def deposit1Max(self) -> int:
        """deposit1Max _summary_

        Returns:
            int: 115792089237 uint256
        """
        if not self._deposit1Max:
            await self.init_deposit1Max()
        return self._deposit1Max

    # v1 contracts have no directDeposit
    @property
    async def directDeposit(self) -> bool:
        """v1 contracts have no directDeposit function

        Returns:
            bool:
        """
        if not self._directDeposit:
            await self.init_directDeposit()
        return self._directDeposit

    @property
    async def fee(self) -> int:
        """fee _summary_

        Returns:
            int: 10 uint8
        """
        if not self._fee:
            await self.init_fee()
        return self._fee

    # v1 contracts have no feeRecipient
    @property
    async def feeRecipient(self) -> str:
        """v1 contracts have no feeRecipient function

        Returns:
            str: address
        """
        if not self._feeRecipient:
            await self.init_feeRecipient()
        return self._feeRecipient

    @property
    async def getBasePosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   287141300490401993 uint128
               amount0     72329994  uint256
               amount1     565062023318300677907  uint256
               }
        """
        if not self._getBasePosition:
            await self.init_getBasePosition()
        return self._getBasePosition

    @property
    async def getLimitPosition(self) -> dict:
        """
        Returns:
           dict:   {
               liquidity   287141300490401993 uint128
               amount0     72329994 uint256
               amount1     565062023318300677907 uint256
               }
        """
        if not self._getLimitPosition:
            await self.init_getLimitPosition()
        return self._getLimitPosition

    @property
    async def getTotalAmounts(self) -> dict:
        """

        Returns:
           _type_: total0   2902086313 uint256
                   total1  565062023318300678136 uint256
        """
        if not self._getTotalAmounts:
            await self.init_getTotalAmounts()
        return self._getTotalAmounts

    @property
    async def limitLower(self) -> int:
        """limitLower _summary_

        Returns:
            int: 0 int24
        """
        if not self._limitLower:
            await self.init_limitLower()
        return self._limitLower

    @property
    async def limitUpper(self) -> int:
        """limitUpper _summary_

        Returns:
            int: 0 int24
        """
        if not self._limitUpper:
            await self.init_limitUpper()
        return self._limitUpper

    @property
    async def maxTotalSupply(self) -> int:
        """maxTotalSupply _summary_

        Returns:
            int: 0 uint256
        """
        if not self._maxTotalSupply:
            await self.init_maxTotalSupply()
        return self._maxTotalSupply

    @property
    async def name(self) -> str:
        if not self._name:
            await self.init_name()
        return self._name

    @property
    async def owner(self) -> str:
        if not self._owner:
            await self.init_owner()
        return self._owner

    @property
    async def pool(self) -> univ3_pool:
        if not self._pool:
            await self.init_pool()
        return self._pool

    @property
    async def tickSpacing(self) -> int:
        """tickSpacing _summary_

        Returns:
            int: 60 int24
        """
        if not self._tickSpacing:
            await self.init_tickSpacing()
        return self._tickSpacing

    @property
    async def token0(self) -> erc20:
        if not self._token0:
            await self.init_token0()
        return self._token0

    @property
    async def token1(self) -> erc20:
        if not self._token1:
            await self.init_token1()
        return self._token1

    async def nonces(self, owner: str):
        return await self.call_function_autoRpc(
            "nonces", None, Web3.to_checksum_address(owner)
        )

    @property
    async def block(self) -> int:
        return await super().block

    @block.setter
    def block(self, value):
        self._block = value
        if self._pool:
            self._pool.block = value
        if self._token0:
            self._token0.block = value
        if self._token1:
            self._token0.block = value

    @property
    async def timestamp(self) -> int:
        """ """
        return await super().timestamp

    @timestamp.setter
    def timestamp(self, value: int):
        self._timestamp = value
        if self._pool:
            self._pool.timestamp = value
        if self._token0:
            self._token0.timestamp = value
        if self._token1:
            self._token1.timestamp = value

    # CUSTOM FUNCTIONS
    def get_all_events(self):
        return NotImplementedError("get_all_events not implemented for v1 contracts")
        # return [
        #     event.createFilter(fromBlock=self.block)
        #     for event in self.contract.events
        #     if issubclass(event, TransactionEvent) # only get transaction events
        # ]

    async def get_qtty_depoloyed(self, inDecimal: bool = True) -> dict:
        """Retrieve the quantity of tokens currently deployed

        Returns:
           dict: {
                   "qtty_token0":0,         # quantity of token 0 deployed in dex
                   "qtty_token1":0,         # quantity of token 1 deployed in dex
                   "fees_owed_token0":0,    # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
                   "fees_owed_token1":0,    # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
                 }
        """

        # init pool and tokens
        pool, baseUpper, baseLower, limitUpper, limitLower = await asyncio.gather(
            self.pool, self.baseUpper, self.baseLower, self.limitUpper, self.limitLower
        )

        base, limit = await asyncio.gather(
            pool.get_qtty_depoloyed(
                ownerAddress=self.address,
                tickUpper=baseUpper,
                tickLower=baseLower,
                inDecimal=inDecimal,
            ),
            pool.get_qtty_depoloyed(
                ownerAddress=self.address,
                tickUpper=limitUpper,
                tickLower=limitLower,
                inDecimal=inDecimal,
            ),
        )

        # add up
        return {k: base.get(k, 0) + limit.get(k, 0) for k in set(base) & set(limit)}

    async def get_fees_uncollected(self, inDecimal: bool = True) -> dict:
        """Retrieve the quantity of fees not collected nor yet owed ( but certain) to the deployed position

        Returns:
            dict: {
                    "qtty_token0":0,  # quantity of uncollected token 0
                    "qtty_token1":0,  # quantity of uncollected token 1
                }
        """

        # init pool and tokens
        pool, baseUpper, baseLower, limitUpper, limitLower = await asyncio.gather(
            self.pool, self.baseUpper, self.baseLower, self.limitUpper, self.limitLower
        )

        # positions
        base, limit = await asyncio.gather(
            pool.get_fees_uncollected(
                ownerAddress=self.address,
                tickUpper=baseUpper,
                tickLower=baseLower,
                inDecimal=inDecimal,
            ),
            pool.get_fees_uncollected(
                ownerAddress=self.address,
                tickUpper=limitUpper,
                tickLower=limitLower,
                inDecimal=inDecimal,
            ),
        )

        return {k: base.get(k, 0) + limit.get(k, 0) for k in set(base) & set(limit)}

    async def get_tvl(self, inDecimal=True) -> dict:
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
        result = {}

        # init pool and tokens
        await asyncio.gather(self.init_pool(), self.init_token0(), self.init_token1())
        pool_token0, pool_token1 = await asyncio.gather(
            self._pool.token0, self._pool.token1
        )
        # get deployed fees as int ( force no decimals)
        (
            deployed,
            result["parked_token0"],
            result["parked_token1"],
        ) = await asyncio.gather(
            self.get_qtty_depoloyed(inDecimal=False),
            pool_token0.balanceOf(self.address),
            pool_token1.balanceOf(self.address),
        )

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
                        10 ** await self._token0.decimals
                    )
                elif "token1" in key:
                    result[key] = Decimal(result[key]) / Decimal(
                        10 ** await self._token1.decimals
                    )
                else:
                    raise ValueError(f"Cant convert '{key}' field to decimal")

        return result.copy()

    async def as_dict(self, convert_bint=False, static_mode: bool = False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): Convert big integers to string. Defaults to False.
            static_mode (bool, optional): only general static fields are returned. Defaults to False.

        Returns:
            dict:
        """
        result = await super().as_dict(convert_bint=convert_bint)

        # init pool
        await self.init_pool()

        (
            result["name"],
            result["fee"],
            result["deposit0Max"],
            result["deposit1Max"],
            result["pool"],
        ) = await asyncio.gather(
            self.name,
            self.fee,
            self.deposit0Max,
            self.deposit1Max,
            self._pool.as_dict(convert_bint=convert_bint, static_mode=static_mode),
        )

        # identify hypervisor dex
        result["dex"] = self.identify_dex_name()

        # result["directDeposit"] = self.directDeposit  # not working

        if convert_bint:
            result["deposit0Max"] = str(result["deposit0Max"])
            result["deposit1Max"] = str(result["deposit1Max"])

        # only return when static mode is off
        if not static_mode:
            await self._as_dict_not_static_items(convert_bint, result)
        return result

    async def _as_dict_not_static_items(self, convert_bint, result):
        (
            result["baseLower"],
            result["baseUpper"],
            result["currentTick"],
            result["limitLower"],
            result["limitUpper"],
            result["maxTotalSupply"],
            result["tvl"],
            result["qtty_depoloyed"],
            result["fees_uncollected"],
            result["basePosition"],
            result["limitPosition"],
            result["tickSpacing"],
            result["totalAmounts"],
        ) = await asyncio.gather(
            self.baseLower,
            self.baseUpper,
            self.currentTick,
            self.limitLower,
            self.limitUpper,
            self.maxTotalSupply,
            self.get_tvl(inDecimal=(not convert_bint)),
            self.get_qtty_depoloyed(inDecimal=(not convert_bint)),
            self.get_fees_uncollected(inDecimal=(not convert_bint)),
            self.getBasePosition,
            self.getLimitPosition,
            self.tickSpacing,
            self.getTotalAmounts,
        )

        if convert_bint:
            result["baseLower"] = str(result["baseLower"])
            result["baseUpper"] = str(result["baseUpper"])
            result["currentTick"] = str(result["currentTick"])
            result["limitLower"] = str(result["limitLower"])
            result["limitUpper"] = str(result["limitUpper"])
            result["totalAmounts"]["total0"] = str(result["totalAmounts"]["total0"])
            result["totalAmounts"]["total1"] = str(result["totalAmounts"]["total1"])
            result["maxTotalSupply"] = str(result["maxTotalSupply"])
            # tvl
            for k in result["tvl"].keys():
                result["tvl"][k] = str(result["tvl"][k])
            # Deployed
            for k in result["qtty_depoloyed"].keys():
                result["qtty_depoloyed"][k] = str(result["qtty_depoloyed"][k])
            # uncollected fees
            for k in result["fees_uncollected"].keys():
                result["fees_uncollected"][k] = str(result["fees_uncollected"][k])

            # positions
            self._as_dict_convert_helper(result, "basePosition")
            self._as_dict_convert_helper(result, "limitPosition")

            result["tickSpacing"] = str(result["tickSpacing"])

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
        timestamp: int = 0,
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
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

    # initializers
    async def init_pool(self):
        self._pool_address = await self.call_function_autoRpc("pool")
        self._pool = algebrav3_pool(
            address=self._pool_address,
            network=self._network,
            block=await self.block,
            timestamp=await self.timestamp,
            custom_web3Url=self.w3.provider.endpoint_uri,
        )


class gamma_hypervisor_quickswap(gamma_hypervisor_algebra):
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
        self._abi_filename = abi_filename or "algebra_hypervisor"
        self._abi_path = abi_path or "sources/common/abis/gamma"

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


class gamma_hypervisor_zyberswap(gamma_hypervisor_algebra):
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
        self._abi_filename = abi_filename or "algebra_hypervisor"
        self._abi_path = abi_path or "sources/common/abis/gamma"

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


class gamma_hypervisor_thena(gamma_hypervisor_algebra):
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
        self._abi_filename = abi_filename or "algebra_hypervisor"
        self._abi_path = abi_path or "sources/common/abis/gamma"

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

    # initializers
    async def init_pool(self):
        self._pool_address = await self.call_function_autoRpc("pool")
        self._pool = algebrav3_pool(
            address=self._pool_address,
            network=self._network,
            block=await self.block,
            timestamp=await self.timestamp,
            abi_filename="albebrav3pool_thena",
            custom_web3Url=self.w3.provider.endpoint_uri,
        )


class gamma_hypervisor_ramses(gamma_hypervisor):
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
        self._abi_filename = abi_filename or "hypervisor"
        self._abi_path = abi_path or "sources/common/abis/ramses"

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

    # initializers
    async def init_pool(self):
        self._pool_address = await self.call_function_autoRpc("pool")
        self._pool = ramses_pool(
            address=self._pool_address,
            network=self._network,
            block=await self.block,
            timestamp=await self.timestamp,
            custom_web3Url=self.w3.provider.endpoint_uri,
        )

    @property
    async def pool(self) -> ramses_pool:
        if not self._pool:
            await self.init_pool()
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
        timestamp: int = 0,
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
            timestamp=timestamp,
            custom_web3=custom_web3,
            custom_web3Url=custom_web3Url,
        )

        self._counter: int | None = None
        self._owner: str | None = None

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
    async def counter(self) -> int:
        """number of hypervisors indexed, initial being 0  and end the counter value

        Returns:
            int: positions of hypervisors in registry
        """
        if not self._counter:
            self._counter = await self.call_function_autoRpc("counter")
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
        return await self.call_function_autoRpc("registry", None, index)

    async def registryMap(self, address: str) -> int:
        return await self.call_function_autoRpc(
            "registryMap", None, Web3.to_checksum_address(address)
        )

    # CUSTOM FUNCTIONS
    async def get_hypervisors(self) -> list[gamma_hypervisor]:
        """Retrieve hypervisors from registry

        Returns:
           gamma_hypervisor
        """
        hypes_list = []
        total_qtty = await self.counter + 1  # index positions ini=0 end=counter

        for i in range(total_qtty):
            try:
                hypervisor_id, idx = await self.hypeByIndex(index=i)

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
                    block=await self.block,
                )
                # check this is actually an hypervisor (erroneous addresses exist like "ethereum":{"0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"})
                await hypervisor.getTotalAmounts  # test func

                # return correct hypervisor
                hypes_list.append(hypervisor)
            except Exception:
                logging.getLogger(__name__).warning(
                    f" Hypervisor registry returned the address {hypervisor_id} and may not be an hypervisor ( at web3 chain id: {self._chain_id} )"
                )

        return hypes_list

    async def get_hypervisors_addresses(self) -> list[str]:
        """Retrieve hypervisors all addresses from registry

        Returns:
           list of addresses
        """

        total_qtty = await self.counter + 1  # index positions ini=0 end=counter

        result = []
        for i in range(total_qtty):
            # executiuon reverted:  arbitrum and mainnet have diff ways of indexing (+1 or 0)
            with contextlib.suppress(Exception):
                hypervisor_id, idx = await self.hypeByIndex(index=i)

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
