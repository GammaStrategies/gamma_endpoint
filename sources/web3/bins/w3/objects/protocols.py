import asyncio
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
        self._baseUpper = await self._contract.functions.baseUpper().call(
            block_identifier=await self.block
        )

    async def init_baseLower(self):
        """baseLower _summary_

        Returns:
            _type_: 0 int24
        """
        self._baseLower = await self._contract.functions.baseLower().call(
            block_identifier=await self.block
        )

    async def init_currentTick(self):
        """currentTick _summary_

        Returns:
            int: -78627 int24
        """
        self._currentTick = await self._contract.functions.currentTick().call(
            block_identifier=await self.block
        )

    async def init_deposit0Max(self):
        """deposit0Max _summary_

        Returns:
            float: 1157920892373161954234007913129639935 uint256
        """
        self._deposit0Max = await self._contract.functions.deposit0Max().call(
            block_identifier=await self.block
        )

    async def init_deposit1Max(self):
        """deposit1Max _summary_

        Returns:
            int: 115792089237 uint256
        """
        self._deposit1Max = await self._contract.functions.deposit1Max().call(
            block_identifier=await self.block
        )

    async def init_directDeposit(self):
        """v1 contracts have no directDeposit function

        Returns:
            bool:
        """
        self._directDeposit = await self._contract.functions.directDeposit().call(
            block_identifier=await self.block
        )

    async def init_fee(self):
        """fee _summary_

        Returns:
            int: 10 uint8
        """
        self._fee = await self._contract.functions.fee().call(
            block_identifier=await self.block
        )

    async def init_feeRecipient(self):
        """v1 contracts have no feeRecipient function

        Returns:
            str: address
        """
        try:
            self._feeRecipient = await self._contract.functions.feeRecipient().call(
                block_identifier=await self.block
            )
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
        tmp = await self._contract.functions.getBasePosition().call(
            block_identifier=await self.block
        )
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
        tmp = await self._contract.functions.getLimitPosition().call(
            block_identifier=await self.block
        )
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
        tmp = await self._contract.functions.getTotalAmounts().call(
            block_identifier=await self.block
        )
        self._getTotalAmounts = {
            "total0": tmp[0],
            "total1": tmp[1],
        }

    async def init_limitLower(self):
        """limitLower _summary_

        Returns:
            int: 0 int24
        """
        self._limitLower = await self._contract.functions.limitLower().call(
            block_identifier=await self.block
        )

    async def init_limitUpper(self):
        """limitUpper _summary_

        Returns:
            int: 0 int24
        """
        self._limitUpper = await self._contract.functions.limitUpper().call(
            block_identifier=await self.block
        )

    async def init_maxTotalSupply(self):
        """maxTotalSupply _summary_

        Returns:
            int: 0 uint256
        """
        self._maxTotalSupply = await self._contract.functions.maxTotalSupply().call(
            block_identifier=await self.block
        )

    async def init_name(self):
        self._name = await self._contract.functions.name().call(
            block_identifier=await self.block
        )

    async def init_owner(self):
        self._owner = await self._contract.functions.owner().call(
            block_identifier=await self.block
        )

    async def init_pool(self):
        self._pool_address = await self._contract.functions.pool().call(
            block_identifier=await self.block
        )
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
        self._tickSpacing = await self._contract.functions.tickSpacing().call(
            block_identifier=await self.block
        )

    async def init_token0(self):
        self._token0_address = await self._contract.functions.token0().call(
            block_identifier=await self.block
        )
        self._token0 = erc20(
            address=self._token0_address,
            network=self._network,
            block=await self.block,
            timestamp=await self.timestamp,
            custom_web3Url=self.w3.provider.endpoint_uri,
        )

    async def init_token1(self):
        self._token1_address = await self._contract.functions.token1().call(
            block_identifier=await self.block
        )
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
        return await self._contract.functions.nonces()(
            Web3.to_checksum_address(owner)
        ).call(block_identifier=await self.block)

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
        # positions

        base, limit = await asyncio.gather(
            self.pool.get_qtty_depoloyed(
                ownerAddress=self.address,
                tickUpper=self.baseUpper,
                tickLower=self.baseLower,
                inDecimal=inDecimal,
            ),
            self.pool.get_qtty_depoloyed(
                ownerAddress=self.address,
                tickUpper=self.limitUpper,
                tickLower=self.limitLower,
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
        # positions
        base, limit = await asyncio.gather(
            self.pool.get_fees_uncollected(
                ownerAddress=self.address,
                tickUpper=self.baseUpper,
                tickLower=self.baseLower,
                inDecimal=inDecimal,
            ),
            self.pool.get_fees_uncollected(
                ownerAddress=self.address,
                tickUpper=self.limitUpper,
                tickLower=self.limitLower,
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

        # get deployed fees as int ( force no decimals)
        (
            deployed,
            result["parked_token0"],
            result["parked_token1"],
        ) = await asyncio.gather(
            self.get_qtty_depoloyed(inDecimal=False),
            self.pool.token0.balanceOf(await self.address),
            self.pool.token1.balanceOf(await self.address),
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
                        10**self._token0.decimals
                    )
                elif "token1" in key:
                    result[key] = Decimal(result[key]) / Decimal(
                        10**self._token1.decimals
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

        (
            result["name"],
            result["fee"],
            result["deposit0Max"],
            result["deposit1Max"],
            result["pool"],
        ) = (
            await asyncio.gather(self.name),
            self.fee,
            self.deposit0Max,
            self.deposit1Max,
            self.pool.as_dict(convert_bint=convert_bint, static_mode=static_mode),
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
        self._pool_address = await self._contract.functions.pool().call(
            block_identifier=await self.block
        )
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
        self._pool_address = await self._contract.functions.pool().call(
            block_identifier=await self.block
        )
        self._pool = algebrav3_pool(
            address=self._pool_address,
            network=self._network,
            block=await self.block,
            timestamp=await self.timestamp,
            abi_filename="albebrav3pool_thena",
            custom_web3Url=self.w3.provider.endpoint_uri,
        )


#####################
#### TODO :   ###############
#################3
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
            self._counter = await self._contract.functions.counter().call(
                block_identifier=await self.block
            )
        return self._counter

    async def hypeByIndex(self, index: int) -> tuple[str, int]:
        """Retrieve hype address and index from registry
            When index is zero, hype address has been deleted so its no longer valid

        Args:
            index (int): index position of hype in registry

        Returns:
            tuple[str, int]: hype address and index
        """
        return await self._contract.functions.hypeByIndex(index).call(
            block_identifier=await self.block
        )

    @property
    async def owner(self) -> str:
        if not self._owner:
            self._owner = await self._contract.functions.owner().call(
                block_identifier=await self.block
            )
        return self._owner

    async def registry(self, index: int) -> str:
        return await self._contract.functions.registry(index).call(
            block_identifier=await self.block
        )

    async def registryMap(self, address: str) -> int:
        return await self._contract.functions.registryMap(
            Web3.to_checksum_address(address)
        ).call(block_identifier=await self.block)

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


##########################################
########################################################
# TODO: async ####################################################################


# rewarders
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
            self._acc_token_precision = (
                await self._contract.functions.ACC_TOKEN_PRECISION().call(
                    block_identifier=await self.block
                )
            )
        return self._acc_token_precision

    @property
    async def masterchef_v2(self) -> str:
        if not self._masterchef_v2:
            self._masterchef_v2 = await self._contract.functions.MASTERCHEF_V2().call(
                block_identifier=await self.block
            )
        return self._masterchef_v2

    @property
    async def funder(self) -> str:
        if not self._funder:
            self._funder = await self._contract.functions.funder().call(
                block_identifier=await self.block
            )
        return self._funder

    @property
    async def owner(self) -> str:
        if not self._owner:
            self._owner = await self._contract.functions.owner().call(
                block_identifier=await self.block
            )
        return self._owner

    @property
    async def pendingOwner(self) -> str:
        if not self._pendingOwner:
            self._pendingOwner = await self._contract.functions.pendingOwner().call(
                block_identifier=await self.block
            )
        return self._pendingOwner

    async def pendingToken(self, pid: int, user: str) -> int:
        return await self._contract.functions.pendingToken(pid, user).call(
            block_identifier=await self.block
        )

    async def pendingTokens(self, pid: int, user: str, input: int) -> tuple[list, list]:
        # rewardTokens address[], rewardAmounts uint256[]
        return await self._contract.functions.pendingTokens(pid, user, input).call(
            block_identifier=await self.block
        )

    async def poolIds(self, input: int) -> int:
        return await self._contract.functions.poolIds(input).call(
            block_identifier=await self.block
        )

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
        return await self._contract.functions.poolInfo(input).call(
            block_identifier=await self.block
        )

    @property
    async def poolLength(self) -> int:
        if not self._poolLength:
            self._poolLength = await self._contract.functions.poolLength().call(
                block_identifier=await self.block
            )
        return self._poolLength

    @property
    async def rewardPerSecond(self) -> int:
        if not self._rewardPerSecond:
            self._rewardPerSecond = (
                await self._contract.functions.rewardPerSecond().call(
                    block_identifier=await self.block
                )
            )
        return self._rewardPerSecond

    @property
    async def rewardToken(self) -> str:
        if not self._rewardToken:
            self._rewardToken = await self._contract.functions.rewardToken().call(
                block_identifier=await self.block
            )
        return self._rewardToken

    @property
    async def totalAllocPoint(self) -> int:
        """Sum of the allocation points of all pools

        Returns:
            int: totalAllocPoint
        """
        if not self._totalAllocPoint:
            self._totalAllocPoint = (
                await self._contract.functions.totalAllocPoint().call(
                    block_identifier=await self.block
                )
            )
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
        return await self._contract.functions.userInfo(pid, user).call(
            block_identifier=await self.block
        )

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
        return await self._contract.functions._getTimeElapsed(
            _from, _to, _endTimestamp
        ).call(block_identifier=await self.block)

    async def currentTimestamp(self, pid: int) -> int:
        return await self._contract.functions._getTimeElapsed(pid).call(
            block_identifier=await self.block
        )

    @property
    async def distributorV2(self) -> str:
        if not self._distributorV2:
            self._distributorV2 = await self._contract.functions.distributorV2().call(
                block_identifier=await self.block
            )
        return self._distributorV2

    @property
    async def isNative(self) -> bool:
        if not self._isNative:
            self._isNative = await self._contract.functions.isNative().call(
                block_identifier=await self.block
            )
        return self._isNative

    @property
    async def owner(self) -> str:
        if not self._owner:
            self._owner = await self._contract.functions.owner().call(
                block_identifier=await self.block
            )
        return self._owner

    async def pendingTokens(self, pid: int, user: str) -> int:
        return await self._contract.functions.pendingTokens(pid, user).call(
            block_identifier=await self.block
        )

    async def poolIds(self, input: int) -> int:
        return await self._contract.functions.poolIds(input).call(
            block_identifier=await self.block
        )

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
        return await self._contract.functions.poolInfo(pid).call(
            block_identifier=await self.block
        )

    async def poolRewardInfo(self, input1: int, input2: int) -> tuple[int, int, int]:
        """_summary_

        Args:
            input1 (int): _description_
            input2 (int): _description_

        Returns:
            tuple[int,int,int]:  startTimestamp uint256, endTimestamp uint256, rewardPerSec uint256
        """
        return await self._contract.functions.poolRewardInfo(input1, input2).call(
            block_identifier=await self.block
        )

    async def poolRewardsPerSec(self, pid: int) -> int:
        return await self._contract.functions.poolRewardsPerSec(pid).call(
            block_identifier=await self.block
        )

    @property
    async def rewardInfoLimit(self) -> int:
        if not self._rewardInfoLimit:
            self._rewardInfoLimit = (
                await self._contract.functions.rewardInfoLimit().call(
                    block_identifier=await self.block
                )
            )
        return self._rewardInfoLimit

    @property
    async def rewardToken(self) -> str:
        if not self._rewardToken:
            self._rewardToken = await self._contract.functions.rewardToken().call(
                block_identifier=await self.block
            )
        return self._rewardToken

    @property
    async def totalAllocPoint(self) -> int:
        """Sum of the allocation points of all pools

        Returns:
            int: totalAllocPoint
        """
        if not self._totalAllocPoint:
            self._totalAllocPoint = (
                await self._contract.functions.totalAllocPoint().call(
                    block_identifier=await self.block
                )
            )
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
        return await self._contract.functions.userInfo(pid, user).call(
            block_identifier=await self.block
        )

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
            self._sushi = await self._contract.functions.SUSHI().call(
                block_identifier=await self.block
            )
        return self._sushi

    async def getRewarder(self, pid: int, rid: int) -> str:
        """Retrieve rewarder address from masterchef

        Args:
            pid (int): The index of the pool
            rid (int): The index of the rewarder

        Returns:
            str: address
        """
        return await self._contract.functions.getRewarder(pid, rid).call(
            block_identifier=await self.block
        )

    async def lpToken(self, pid: int) -> str:
        """Retrieve lp token address (hypervisor) from masterchef

        Args:
            index (int): index of the pool ( same of rewarder )

        Returns:
            str:  hypervisor address ( LP token)
        """
        return await self._contract.functions.lpToken(pid).call(
            block_identifier=await self.block
        )

    @property
    async def owner(self) -> str:
        if not self._owner:
            self._owner = await self._contract.functions.owner().call(
                block_identifier=await self.block
            )
        return self._owner

    @property
    async def pendingOwner(self) -> str:
        if not self._pendingOwner:
            self._pendingOwner = await self._contract.functions.pendingOwner().call(
                block_identifier=await self.block
            )
        return self._pendingOwner

    @property
    async def pendingSushi(self, pid: int, user: str) -> int:
        """pending SUSHI reward for a given user

        Args:
            pid (int): The index of the pool
            user (str):  address

        Returns:
            int: _description_
        """
        return await self._contract.functions.pendingSushi(pid, user).call(
            block_identifier=await self.block
        )

    async def poolInfo(self, pid: int) -> tuple[int, int, int]:
        """_summary_

        Returns:
            tuple[int,int,int]:  accSushiPerShare uint128, lastRewardTime uint64, allocPoint uint64
        """
        return await self._contract.functions.poolInfo(pid).call(
            block_identifier=await self.block
        )

    @property
    async def poolLength(self) -> int:
        """Returns the number of MCV2 pools
        Returns:
            int:
        """
        if not self._poolLength:
            self._poolLength = await self._contract.functions.poolLength().call(
                block_identifier=await self.block
            )
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
        return await self._contract.functions.deposited(pid, user).call(
            block_identifier=await self.block
        )

    @property
    async def endTimestamp(self) -> int:
        """_summary_

        Returns:
            int: _description_
        """
        if not self._endTimestamp:
            self._endTimestamp = await self._contract.functions.endTimestamp().call(
                block_identifier=await self.block
            )
        return self._endTimestamp

    @property
    async def erc20(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._erc20:
            self._erc20 = await self._contract.functions.erc20().call(
                block_identifier=await self.block
            )
        return self._erc20

    @property
    async def feeAddress(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._feeAddress:
            self._feeAddress = await self._contract.functions.feeAddress().call(
                block_identifier=await self.block
            )
        return self._feeAddress

    @property
    async def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._owner:
            self._owner = await self._contract.functions.owner().call(
                block_identifier=await self.block
            )
        return self._owner

    @property
    async def paidOut(self) -> int:
        """_summary_

        Returns:
            int: _description_
        """
        if not self._paidOut:
            self._paidOut = await self._contract.functions.paidOut().call(
                block_identifier=await self.block
            )
        return self._paidOut

    async def pending(self, pid: int, user: str) -> int:
        """_summary_

        Args:
            pid (int): pool index
            user (str): address

        Returns:
            int: _description_
        """
        return await self._contract.functions.pending(pid, user).call(
            block_identifier=await self.block
        )

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
        return await self._contract.functions.poolInfo(pid).call(
            block_identifier=await self.block
        )

    @property
    async def poolLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._poolLength:
            self._poolLength = await self._contract.functions.poolLength().call(
                block_identifier=await self.block
            )
        return self._poolLength

    @property
    async def rewardPerSecond(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._rewardPerSecond:
            self._rewardPerSecond = (
                await self._contract.functions.rewardPerSecond().call(
                    block_identifier=await self.block
                )
            )
        return self._rewardPerSecond

    @property
    async def startTimestamp(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._startTimestamp:
            self._startTimestamp = await self._contract.functions.startTimestamp().call(
                block_identifier=await self.block
            )
        return self._startTimestamp

    @property
    async def totalAllocPoint(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._totalAllocPoint:
            self._totalAllocPoint = (
                await self._contract.functions.totalAllocPoint().call(
                    block_identifier=await self.block
                )
            )
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
        return await self._contract.functions.userInfo(pid, user).call(
            block_identifier=await self.block
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
            self._maximum_deposit_fee_rate = (
                await self._contract.functions.MAXIMUM_DEPOSIT_FEE_RATE().call(
                    block_identifier=await self.block
                )
            )
        return self._maximum_deposit_fee_rate

    @property
    async def maximum_harvest_interval(self) -> int:
        """maximum harvest interval

        Returns:
            int: unit256
        """
        if not self._maximum_harvest_interval:
            self._maximum_harvest_interval = (
                await self._contract.functions.MAXIMUM_HARVEST_INTERVAL().call(
                    block_identifier=await self.block
                )
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
        return await self._contract.functions.canHarvest(pid, user).call(
            block_identifier=await self.block
        )

    @property
    async def feeAddress(self) -> str:
        """fee address

        Returns:
            str: address
        """
        if not self._feeAddress:
            self._feeAddress = await self._contract.functions.feeAddress().call(
                block_identifier=await self.block
            )
        return self._feeAddress

    @property
    async def getZyberPerSec(self) -> int:
        """zyber per sec

        Returns:
            int: unit256
        """
        if not self._getZyberPerSecond:
            self._getZyberPerSecond = (
                await self._contract.functions.getZyberPerSec().call(
                    block_identifier=await self.block
                )
            )
        return self._getZyberPerSecond

    @property
    async def marketingAddress(self) -> str:
        """marketing address

        Returns:
            str: address
        """
        if not self._marketingAddress:
            self._marketingAddress = (
                await self._contract.functions.marketingAddress().call(
                    block_identifier=await self.block
                )
            )
        return self._marketingAddress

    @property
    async def marketingPercent(self) -> int:
        """marketing percent

        Returns:
            int: unit256
        """
        if not self._marketingPercent:
            self._marketingPercent = (
                await self._contract.functions.marketingPercent().call(
                    block_identifier=await self.block
                )
            )
        return self._marketingPercent

    @property
    async def owner(self) -> str:
        """owner

        Returns:
            str: address
        """
        if not self._owner:
            self._owner = await self._contract.functions.owner().call(
                block_identifier=await self.block
            )
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
        return await self._contract.functions.pendingTokens(pid, user).call(
            block_identifier=await self.block
        )

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
        return await self._contract.functions.poolInfo(pid).call(
            block_identifier=await self.block
        )

    @property
    async def poolLength(self) -> int:
        """pool length

        Returns:
            int: unit256
        """
        if not self._poolLength:
            self._poolLength = await self._contract.functions.poolLength().call(
                block_identifier=await self.block
            )
        return self._poolLength

    async def poolRewarders(self, pid: int) -> list[str]:
        """pool rewarders

        Args:
            pid (int): pool id

        Returns:
            list[str]: address[]
        """
        return await self._contract.functions.poolRewarders(pid).call(
            block_identifier=await self.block
        )

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
        return await self._contract.functions.poolRewardsPerSec(pid).call(
            block_identifier=await self.block
        )

    async def poolTotalLp(self, pid: int) -> int:
        """pool total lp

        Args:
            pid (int): pool id

        Returns:
            int: unit256
        """
        return await self._contract.functions.poolTotalLp(pid).call(
            block_identifier=await self.block
        )

    @property
    async def startTimestamp(self) -> int:
        """start timestamp

        Returns:
            int: unit256
        """
        if not self._startTimestamp:
            self._startTimestamp = await self._contract.functions.startTimestamp().call(
                block_identifier=await self.block
            )
        return self._startTimestamp

    @property
    async def teamAddress(self) -> str:
        """team address

        Returns:
            str: address
        """
        if not self._teamAddress:
            self._teamAddress = await self._contract.functions.teamAddress().call(
                block_identifier=await self.block
            )
        return self._teamAddress

    @property
    async def teamPercent(self) -> int:
        """team percent

        Returns:
            int: unit256
        """
        if not self._teamPercent:
            self._teamPercent = await self._contract.functions.teamPercent().call(
                block_identifier=await self.block
            )
        return self._teamPercent

    @property
    async def totalAllocPoint(self) -> int:
        """total alloc point

        Returns:
            int: unit256
        """
        if not self._totalAllocPoint:
            self._totalAllocPoint = (
                await self._contract.functions.totalAllocPoint().call(
                    block_identifier=await self.block
                )
            )
        return self._totalAllocPoint

    @property
    async def totalLockedUpRewards(self) -> int:
        """total locked up rewards

        Returns:
            int: unit256
        """
        if not self._totalLockedUpRewards:
            self._totalLockedUpRewards = (
                await self._contract.functions.totalLockedUpRewards().call(
                    block_identifier=await self.block
                )
            )
        return self._totalLockedUpRewards

    @property
    async def totalZyberInPools(self) -> int:
        """total zyber in pools

        Returns:
            int: unit256
        """
        if not self._totalZyberInPools:
            self._totalZyberInPools = (
                await self._contract.functions.totalZyberInPools().call(
                    block_identifier=await self.block
                )
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
        return await self._contract.functions.userInfo(pid, user).call(
            block_identifier=await self.block
        )

    @property
    async def zyber(self) -> str:
        """zyber

        Returns:
            str: address
        """
        if not self._zyber:
            self._zyber = await self._contract.functions.zyber().call(
                block_identifier=await self.block
            )
        return self._zyber

    @property
    async def zyberPerSec(self) -> int:
        """zyber per sec

        Returns:
            int: unit256
        """
        if not self._zyberPerSec:
            self._zyberPerSec = await self._contract.functions.zyberPerSec().call(
                block_identifier=await self.block
            )
        return self._zyberPerSec


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
            counter = await self._contract.functions.counter().call(
                block_identifier=await self.block
            )
        return self._counter

    async def hypeByIndex(self, index: int) -> tuple[str, int]:
        """Retrieve hype address and index from registry
            When index is zero, hype address has been deleted so its no longer valid

        Args:
            index (int): index position of hype in registry

        Returns:
            tuple[str, int]: hype address and index
        """
        return await self._contract.functions.hypeByIndex(index).call(
            block_identifier=await self.block
        )

    @property
    async def owner(self) -> str:
        if not self._owner:
            self._owner = await self._contract.functions.owner().call(
                block_identifier=await self.block
            )
        return self._owner

    async def registry(self, index: int) -> str:
        return await self._contract.functions.registry(index).call(
            block_identifier=await self.block
        )

    async def registryMap(self, address: str) -> int:
        return await self._contract.functions.registryMap(
            Web3.to_checksum_address(address)
        ).call(block_identifier=await self.block)

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
        self._epochTimestamp: int | None = None
        self._factories: list[str] | None = None
        self._ve: str | None = None
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
            self._max_vote_delay = await self._contract.functions.MAX_VOTE_DELAY().call(
                block_identifier=await self.block
            )
        return self._max_vote_delay

    @property
    async def vote_delay(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._vote_delay:
            self._vote_delay = await self._contract.functions.VOTE_DELAY().call(
                block_identifier=await self.block
            )
        return self._vote_delay

    @property
    async def _epochTimestamp(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._epochTimestamp:
            self._epochTimestamp = (
                await self._contract.functions._epochTimestamp().call(
                    block_identifier=await self.block
                )
            )
        return self._epochTimestamp

    @property
    async def _factories(self) -> list[str]:
        """_summary_

        Returns:
            list[str]: address[]
        """
        if not self._factories:
            self._factories = await self._contract.functions._factories().call(
                block_identifier=await self.block
            )
        return self._factories

    @property
    async def _ve(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._ve:
            self._ve = await self._contract.functions._ve().call(
                block_identifier=await self.block
            )
        return self._ve

    @property
    async def bribefactory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._bribefactory:
            self._bribefactory = await self._contract.functions.bribefactory().call(
                block_identifier=await self.block
            )
        return self._bribefactory

    async def claimable(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return await self._contract.functions.claimable(address).call(
            block_identifier=await self.block
        )

    async def external_bribes(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return await self._contract.functions.external_bribes(address).call(
            block_identifier=await self.block
        )

    async def factories(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return await self._contract.functions.factories(index).call(
            block_identifier=await self.block
        )

    @property
    async def factory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._factory:
            self._factory = await self._contract.functions.factory().call(
                block_identifier=await self.block
            )
        return self._factory

    @property
    async def factoryLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._factoryLength:
            self._factoryLength = await self._contract.functions.factoryLength().call(
                block_identifier=await self.block
            )
        return self._factoryLength

    async def gaugeFactories(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return await self._contract.functions.gaugeFactories(index).call(
            block_identifier=await self.block
        )

    @property
    async def gaugeFactoriesLength(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._gaugeFactoriesLength:
            self._gaugeFactoriesLength = (
                await self._contract.functions.gaugeFactoriesLength().call(
                    block_identifier=await self.block
                )
            )
        return self._gaugeFactoriesLength

    @property
    async def gaugefactory(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._gaugefactory:
            self._gaugefactory = await self._contract.functions.gaugefactory().call(
                block_identifier=await self.block
            )
        return self._gaugefactory

    async def gauges(self, address: str) -> str:
        """_summary_

        Args:
            address (str):

        Returns:
            str: address
        """
        return await self._contract.functions.gauges(address).call(
            block_identifier=await self.block
        )

    async def gaugesDistributionTimestamp(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return await self._contract.functions.gaugesDistributionTimestamp(address).call(
            block_identifier=await self.block
        )

    async def internal_bribes(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return await self._contract.functions.internal_bribes(address).call(
            block_identifier=await self.block
        )

    @property
    async def isAlive(self) -> bool:
        """_summary_

        Returns:
            bool: bool
        """
        if not self._isAlive:
            self._isAlive = await self._contract.functions.isAlive().call(
                block_identifier=await self.block
            )
        return self._isAlive

    async def isFactory(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return await self._contract.functions.isFactory(address).call(
            block_identifier=await self.block
        )

    async def isGauge(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return await self._contract.functions.isGauge(address).call(
            block_identifier=await self.block
        )

    async def isGaugeFactory(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return await self._contract.functions.isGaugeFactory(address).call(
            block_identifier=await self.block
        )

    async def isWhitelisted(self, address: str) -> bool:
        """_summary_

        Args:
            address (str): address

        Returns:
            bool: bool
        """
        return await self._contract.functions.isWhitelisted(address).call(
            block_identifier=await self.block
        )

    async def lastVoted(self, index: int) -> int:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            int: uint256
        """
        return await self._contract.functions.lastVoted(index).call(
            block_identifier=await self.block
        )

    @property
    async def length(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._length:
            self._length = await self._contract.functions.length().call(
                block_identifier=await self.block
            )
        return self._length

    @property
    async def minter(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._minter:
            self._minter = await self._contract.functions.minter().call(
                block_identifier=await self.block
            )
        return self._minter

    @property
    async def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._owner:
            self._owner = await self._contract.functions.owner().call(
                block_identifier=await self.block
            )
        return self._owner

    @property
    async def permissionRegistry(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._permissionRegistry:
            self._permissionRegistry = (
                await self._contract.functions.permissionRegistry().call(
                    block_identifier=await self.block
                )
            )
        return self._permissionRegistry

    async def poolForGauge(self, address: str) -> str:
        """_summary_

        Args:
            address (str): address

        Returns:
            str: address
        """
        return await self._contract.functions.poolForGauge(address).call(
            block_identifier=await self.block
        )

    async def poolVote(self, input1: int, input2: int) -> str:
        """_summary_

        Args:
            input1 (int): uint256
            input2 (int): uint256

        Returns:
            str: address
        """
        return await self._contract.functions.poolVote(input1, input2).call(
            block_identifier=await self.block
        )

    async def poolVoteLength(self, tokenId: int) -> int:
        """_summary_

        Args:
            tokenId (int): uint256

        Returns:
            int: uint256
        """
        return await self._contract.functions.poolVoteLength(tokenId).call(
            block_identifier=await self.block
        )

    async def pools(self, index: int) -> str:
        """_summary_

        Args:
            index (int): uint256

        Returns:
            str: address
        """
        return await self._contract.functions.pools(index).call(
            block_identifier=await self.block
        )

    @property
    async def totalWeight(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._totalWeight:
            self._totalWeight = await self._contract.functions.totalWeight().call(
                block_identifier=await self.block
            )
        return self._totalWeight

    async def totalWeightAt(self, time: int) -> int:
        """_summary_

        Args:
            time (int): uint256

        Returns:
            int: uint256
        """
        return await self._contract.functions.totalWeightAt(time).call(
            block_identifier=await self.block
        )

    async def usedWeights(self, index: int) -> int:
        """_summary_

        Args:
            index (int)

        Returns:
            int: uint256
        """
        return await self._contract.functions.usedWeights(index).call(
            block_identifier=await self.block
        )

    async def votes(self, index: int, address: str) -> int:
        """_summary_

        Args:
            index (int): uint256
            address (str): address

        Returns:
            int: uint256
        """
        return await self._contract.functions.votes(index, address).call(
            block_identifier=await self.block
        )

    async def weights(self, pool_address: str) -> int:
        """_summary_

        Args:
            pool_address (str): address

        Returns:
            int: uint256
        """
        return await self._contract.functions.weights(pool_address).call(
            block_identifier=await self.block
        )

    async def weightsAt(self, pool_address: str, time: int) -> int:
        """_summary_

        Args:
            pool_address (str): address
            time (int): uint256

        Returns:
            int: uint256
        """
        return await self._contract.functions.weightsAt(pool_address, time).call(
            block_identifier=await self.block
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
        self._ve: str | None = None
        self.__periodFinish: int | None = None
        self.__totalSupply: int | None = None
        self._emergency: bool | None = None
        self.external_bribe: str | None = None
        self._feeVault: str | None = None
        self._fees0: int | None = None
        self._fees1: int | None = None
        self._gaugeRewarder: str | None = None
        self._internal_bribe: str | None = None
        self._lastTimeRewardApplicable: int | None = None
        self._lastUpdateTime: int | None = None
        self._owner: str | None = None
        self._periodFinish: int | None = None
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
            self._distribution = await self._contract.functions.DISTRIBUTION().call(
                block_identifier=await self.block
            )
        return self._distribution

    @property
    async def duration(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._duration:
            self._duration = await self._contract.functions.DURATION().call(
                block_identifier=await self.block
            )
        return self._duration

    @property
    async def token(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._token:
            self._token = await self._contract.functions.TOKEN().call(
                block_identifier=await self.block
            )
        return self._token

    @property
    async def _ve(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._ve:
            self._ve = await self._contract.functions._ve().call(
                block_identifier=await self.block
            )
        return self._ve

    async def _balances(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return await self._contract.functions._balances(address).call(
            block_identifier=await self.block
        )

    @property
    async def _periodFinish(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self.__periodFinish:
            self.__periodFinish = await self._contract.functions._periodFinish().call(
                block_identifier=await self.block
            )
        return self.__periodFinish

    @property
    async def _totalSupply(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self.__totalSupply:
            self.__totalSupply = await self._contract.functions._totalSupply().call(
                block_identifier=await self.block
            )
        return self.__totalSupply

    async def balanceOf(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return await self._contract.functions.balanceOf(address).call(
            block_identifier=await self.block
        )

    async def earned(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return await self._contract.functions.earned(address).call(
            block_identifier=await self.block
        )

    @property
    async def emergency(self) -> bool:
        """_summary_

        Returns:
            bool: bool
        """
        if not self._emergency:
            self._emergency = await self._contract.functions.emergency().call(
                block_identifier=await self.block
            )
        return self._emergency

    @property
    async def external_bribe(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._external_bribe:
            self._external_bribe = await self._contract.functions.external_bribe().call(
                block_identifier=await self.block
            )
        return self._external_bribe

    @property
    async def feeVault(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._feeVault:
            self._feeVault = await self._contract.functions.feeVault().call(
                block_identifier=await self.block
            )
        return self._feeVault

    @property
    async def fees0(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._fees0:
            self._fees0 = await self._contract.functions.fees0().call(
                block_identifier=await self.block
            )
        return self._fees0

    @property
    async def fees1(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._fees1:
            self._fees1 = await self._contract.functions.fees1().call(
                block_identifier=await self.block
            )
        return self._fees1

    @property
    async def gaugeRewarder(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._gaugeRewarder:
            self._gaugeRewarder = await self._contract.functions.gaugeRewarder().call(
                block_identifier=await self.block
            )
        return self._gaugeRewarder

    @property
    async def internal_bribe(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._internal_bribe:
            self._internal_bribe = await self._contract.functions.internal_bribe().call(
                block_identifier=await self.block
            )
        return self._internal_bribe

    @property
    async def lastTimeRewardApplicable(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._lastTimeRewardApplicable:
            self._lastTimeRewardApplicable = (
                await self._contract.functions.lastTimeRewardApplicable().call(
                    block_identifier=await self.block
                )
            )
        return self._lastTimeRewardApplicable

    @property
    async def lastUpdateTime(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._lastUpdateTime:
            self._lastUpdateTime = await self._contract.functions.lastUpdateTime().call(
                block_identifier=await self.block
            )
        return self._lastUpdateTime

    @property
    async def owner(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._owner:
            self._owner = await self._contract.functions.owner().call(
                block_identifier=await self.block
            )
        return self._owner

    @property
    async def periodFinish(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._periodFinish:
            self._periodFinish = await self._contract.functions.periodFinish().call(
                block_identifier=await self.block
            )
        return self._periodFinish

    @property
    async def rewardPerDuration(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._rewardPerDuration:
            self._rewardPerDuration = (
                await self._contract.functions.rewardPerDuration().call(
                    block_identifier=await self.block
                )
            )
        return self._rewardPerDuration

    @property
    async def rewardPerToken(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._rewardPerToken:
            self._rewardPerToken = await self._contract.functions.rewardPerToken().call(
                block_identifier=await self.block
            )
        return self._rewardPerToken

    @property
    async def rewardPerTokenStored(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._rewardPerTokenStored:
            self._rewardPerTokenStored = (
                await self._contract.functions.rewardPerTokenStored().call(
                    block_identifier=await self.block
                )
            )
        return self._rewardPerTokenStored

    @property
    async def rewardRate(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._rewardRate:
            self._rewardRate = await self._contract.functions.rewardRate().call(
                block_identifier=await self.block
            )
        return self._rewardRate

    @property
    async def rewardToken(self) -> str:
        """_summary_

        Returns:
            str: address
        """
        if not self._rewardToken:
            self._rewardToken = await self._contract.functions.rewardToken().call(
                block_identifier=await self.block
            )
        return self._rewardToken

    @property
    async def rewardPid(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._rewardPid:
            self._rewardPid = await self._contract.functions.rewardPid().call(
                block_identifier=await self.block
            )
        return self._rewardPid

    async def rewards(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return await self._contract.functions.rewards(address).call(
            block_identifier=await self.block
        )

    @property
    async def totalSupply(self) -> int:
        """_summary_

        Returns:
            int: uint256
        """
        if not self._totalSupply:
            self._totalSupply = await self._contract.functions.totalSupply().call(
                block_identifier=await self.block
            )
        return self._totalSupply

    async def userRewardPerTokenPaid(self, address: str) -> int:
        """_summary_

        Args:
            address (str): address

        Returns:
            int: uint256
        """
        return await self._contract.functions.userRewardPerTokenPaid(address).call(
            block_identifier=await self.block
        )
