import logging
import sys
import asyncio

from decimal import Decimal
from hexbytes import HexBytes
from web3 import Web3

from sources.web3.bins.formulas import univ3_formulas
from sources.web3.bins.w3.objects.basic import web3wrap, erc20


class univ3_pool(web3wrap):
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
        self._abi_filename = abi_filename or "univ3_pool"
        self._abi_path = abi_path or "sources/common/abis/uniswap/v3"

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

        self._factory: str = None
        self._fee: int = None
        self._feeGrowthGlobal0X128: int = None
        self._feeGrowthGlobal1X128: int = None
        self._liquidity: int = None
        self._maxLiquidityPerTick: int = None
        self._protocolFees: int = None
        self._slot0: dict = None
        self._tick: int = None
        self._tickCurrent: int = None
        self._tickSpacing: int = None
        self._token0: erc20 = None
        self._token1: erc20 = None

    async def init_factory(self):
        self._factory = await self._contract.functions.factory().call(
            block_identifier=await self.block
        )

    async def init_fee(self):
        self._fee = await self._contract.functions.fee().call(
            block_identifier=await self.block
        )

    async def init_feeGrowthGlobal0X128(self):
        self._feeGrowthGlobal0X128 = (
            await self._contract.functions.feeGrowthGlobal0X128().call(
                block_identifier=await self.block
            )
        )

    async def init_feeGrowthGlobal1X128(self):
        self._feeGrowthGlobal1X128 = (
            await self._contract.functions.feeGrowthGlobal1X128().call(
                block_identifier=await self.block
            )
        )

    async def init_liquidity(self):
        self._liquidity = await self._contract.functions.liquidity().call(
            block_identifier=await self.block
        )

    async def init_maxLiquidityPerTick(self):
        self._maxLiquidityPerTick = (
            await self._contract.functions.maxLiquidityPerTick().call(
                block_identifier=await self.block
            )
        )

    async def init_protocolFees(self):
        self._protocolFees = await self._contract.functions.protocolFees().call(
            block_identifier=await self.block
        )

    async def init_slot0(self):
        """The 0th storage slot in the pool stores many values, and is exposed as a single method to save gas when accessed externally.

        Returns:
           _type_: sqrtPriceX96   uint160 :  28854610805518743926885543006518067
                   tick   int24 :  256121
                   observationIndex   uint16 :  198
                   observationCardinality   uint16 :  300
                   observationCardinalityNext   uint16 :  300
                   feeProtocol   uint8 :  0
                   unlocked   bool :  true
        """
        tmp = await self._contract.functions.slot0().call(
            block_identifier=await self.block
        )
        self._slot0 = {
            "sqrtPriceX96": tmp[0],
            "tick": tmp[1],
            "observationIndex": tmp[2],
            "observationCardinality": tmp[3],
            "observationCardinalityNext": tmp[4],
            "feeProtocol": tmp[5],
            "unlocked": tmp[6],
        }

    async def init_tickSpacing(self):
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
    async def factory(self) -> str:
        if not self._factory:
            await self.init_factory()
        return self._factory

    @property
    async def fee(self) -> int:
        if not self._fee:
            await self.init_fee()
        return self._fee

    @property
    async def feeGrowthGlobal0X128(self) -> int:
        if not self._feeGrowthGlobal0X128:
            await self.init_feeGrowthGlobal0X128()
        return self._feeGrowthGlobal0X128

    @property
    async def feeGrowthGlobal1X128(self) -> int:
        if not self._feeGrowthGlobal1X128:
            await self.init_feeGrowthGlobal1X128()
        return self._feeGrowthGlobal1X128

    @property
    async def liquidity(self) -> int:
        if not self._liquidity:
            await self.init_liquidity()
        return self._liquidity

    @property
    async def maxLiquidityPerTick(self) -> int:
        if not self._maxLiquidityPerTick:
            await self.init_maxLiquidityPerTick()
        return self._maxLiquidityPerTick

    @property
    async def protocolFees(self) -> int:
        if not self._protocolFees:
            await self.init_protocolFees()
        return self._protocolFees

    @property
    async def slot0(self) -> dict:
        if not self._slot0:
            await self.init_slot0()
        return self._slot0

    @property
    async def tickSpacing(self) -> int:
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

    async def observations(self, input: int):
        return await self._contract.functions.observations(input).call(
            block_identifier=await self.block
        )

    async def observe(self, secondsAgo: int):
        """observe _summary_

        Args:
           secondsAgo (int): _description_

        Returns:
           _type_: tickCumulatives   int56[] :  12731930095582
                   secondsPerLiquidityCumulativeX128s   uint160[] :  242821134689165142944235398318169

        """
        return await self._contract.functions.observe(secondsAgo).call(
            block_identifier=await self.block
        )

    async def positions(self, position_key: str) -> dict:
        """

        Args:
           position_key (str): 0x....

        Returns:
           _type_:
                   liquidity   uint128 :  99225286851746
                   feeGrowthInside0LastX128   uint256 :  0
                   feeGrowthInside1LastX128   uint256 :  0
                   tokensOwed0   uint128 :  0
                   tokensOwed1   uint128 :  0
        """
        result = await self._contract.functions.positions(position_key).call(
            block_identifier=await self.block
        )
        return {
            "liquidity": result[0],
            "feeGrowthInside0LastX128": result[1],
            "feeGrowthInside1LastX128": result[2],
            "tokensOwed0": result[3],
            "tokensOwed1": result[4],
        }

    async def snapshotCumulativeInside(self, tickLower: int, tickUpper: int):
        return await self._contract.functions.snapshotCumulativeInside(
            tickLower, tickUpper
        ).call(block_identifier=await self.block)

    async def tickBitmap(self, input: int) -> int:
        return await self._contract.functions.tickBitmap(input).call(
            block_identifier=await self.block
        )

    async def ticks(self, tick: int) -> dict:
        """

        Args:
           tick (int):

        Returns:
           _type_:     liquidityGross   uint128 :  0
                       liquidityNet   int128 :  0
                       feeGrowthOutside0X128   uint256 :  0
                       feeGrowthOutside1X128   uint256 :  0
                       tickCumulativeOutside   int56 :  0
                       spoolecondsPerLiquidityOutsideX128   uint160 :  0
                       secondsOutside   uint32 :  0
                       initialized   bool :  false
        """
        result = await self._contract.functions.ticks(tick).call(
            block_identifier=await self.block
        )
        return {
            "liquidityGross": result[0],
            "liquidityNet": result[1],
            "feeGrowthOutside0X128": result[2],
            "feeGrowthOutside1X128": result[3],
            "tickCumulativeOutside": result[4],
            "secondsPerLiquidityOutsideX128": result[5],
            "secondsOutside": result[6],
            "initialized": result[7],
        }

    # write function without state change ( not wrkin)
    async def collect(
        self, recipient, tickLower, tickUpper, amount0Requested, amount1Requested, owner
    ):
        return await self._contract.functions.collect(
            recipient, tickLower, tickUpper, amount0Requested, amount1Requested
        ).call({"from": owner})

    # CUSTOM PROPERTIES
    @property
    async def block(self) -> int:
        return await super().block

    @block.setter
    def block(self, value: int):
        # set block
        self._block = value
        if self._token0:
            self._token0.block = value
        if self._token1:
            self._token1.block = value

    @property
    async def timestamp(self) -> int:
        """ """
        return await super().timestamp

    @timestamp.setter
    def timestamp(self, value: int):
        self._timestamp = value
        if self._token0:
            self._token0.timestamp = value
        if self._token1:
            self._token1.timestamp = value

    # CUSTOM FUNCTIONS
    async def position(self, ownerAddress: str, tickLower: int, tickUpper: int) -> dict:
        """

        Returns:
           dict:
                   liquidity   uint128 :  99225286851746
                   feeGrowthInside0LastX128   uint256 :  0
                   feeGrowthInside1LastX128   uint256 :  0
                   tokensOwed0   uint128 :  0
                   tokensOwed1   uint128 :  0
        """
        return await self.positions(
            univ3_formulas.get_positionKey(
                ownerAddress=ownerAddress,
                tickLower=tickLower,
                tickUpper=tickUpper,
            )
        )

    async def get_qtty_depoloyed(
        self, ownerAddress: str, tickUpper: int, tickLower: int, inDecimal: bool = True
    ) -> dict:
        """Retrieve the quantity of tokens currently deployed

        Args:
           ownerAddress (str):
           tickUpper (int):
           tickLower (int):
           inDecimal (bool): return result in a decimal format?

        Returns:
           dict: {
                   "qtty_token0":0,        (int or Decimal) # quantity of token 0 deployed in dex
                   "qtty_token1":0,        (int or Decimal) # quantity of token 1 deployed in dex
                   "fees_owed_token0":0,   (int or Decimal) # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
                   "fees_owed_token1":0,   (int or Decimal) # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
                 }
        """

        result = {
            "qtty_token0": 0,  # quantity of token 0 deployed in dex
            "qtty_token1": 0,  # quantity of token 1 deployed in dex
            "fees_owed_token0": 0,  # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
            "fees_owed_token1": 0,  # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
        }

        # get position and slot data
        pos, slot0 = await asyncio.gather(
            self.position(
                ownerAddress=Web3.to_checksum_address(ownerAddress.lower()),
                tickLower=tickLower,
                tickUpper=tickUpper,
            ),
            self.slot0,
        )

        # get current tick from slot
        tickCurrent = slot0["tick"]
        sqrtRatioX96 = slot0["sqrtPriceX96"]
        sqrtRatioAX96 = univ3_formulas.TickMath.getSqrtRatioAtTick(tickLower)
        sqrtRatioBX96 = univ3_formulas.TickMath.getSqrtRatioAtTick(tickUpper)
        # calc quantity from liquidity
        (
            result["qtty_token0"],
            result["qtty_token1"],
        ) = univ3_formulas.LiquidityAmounts.getAmountsForLiquidity(
            sqrtRatioX96, sqrtRatioAX96, sqrtRatioBX96, pos["liquidity"]
        )

        # add owed tokens
        result["fees_owed_token0"] = pos["tokensOwed0"]
        result["fees_owed_token1"] = pos["tokensOwed1"]

        # convert to decimal as needed
        if inDecimal:
            await self._get_qtty_depoloyed_todecimal(result)
        # return result
        return result.copy()

    async def _get_qtty_depoloyed_todecimal(self, result):
        # get token decimals
        decimals_token0, decimals_token1 = await asyncio.gather(
            self.token0.decimals, self.token1.decimals
        )

        result["qtty_token0"] = Decimal(result["qtty_token0"]) / Decimal(
            10**decimals_token0
        )
        result["qtty_token1"] = Decimal(result["qtty_token1"]) / Decimal(
            10**decimals_token1
        )
        result["fees_owed_token0"] = Decimal(result["fees_owed_token0"]) / Decimal(
            10**decimals_token0
        )
        result["fees_owed_token1"] = Decimal(result["fees_owed_token1"]) / Decimal(
            10**decimals_token1
        )

    async def get_fees_uncollected(
        self, ownerAddress: str, tickUpper: int, tickLower: int, inDecimal: bool = True
    ) -> dict:
        """Retrieve the quantity of fees not collected nor yet owed ( but certain) to the deployed position

        Args:
            ownerAddress (str):
            tickUpper (int):
            tickLower (int):
            inDecimal (bool): return result in a decimal format?

        Returns:
            dict: {
                    "qtty_token0":0,   (int or Decimal)     # quantity of uncollected token 0
                    "qtty_token1":0,   (int or Decimal)     # quantity of uncollected token 1
                }
        """

        result = {
            "qtty_token0": 0,
            "qtty_token1": 0,
            "qtty_token0_owed": 0,
            "qtty_token1_owed": 0,
        }

        # get position data and ticks
        (
            pos,
            ticks_lower,
            ticks_upper,
            slot0,
            feeGrowthGlobal0X128,
            feeGrowthGlobal1X128,
        ) = await asyncio.gather(
            self.position(
                ownerAddress=Web3.to_checksum_address(ownerAddress.lower()),
                tickLower=tickLower,
                tickUpper=tickUpper,
            ),
            self.ticks(tickLower),
            self.ticks(tickUpper),
            self.slot0,
            self.feeGrowthGlobal0X128,
            self.feeGrowthGlobal1X128,
        )
        tickCurrent = slot0["tick"]

        # save tokens owed
        result["qtty_token0_owed"] = pos["tokensOwed0"]
        result["qtty_token1_owed"] = pos["tokensOwed1"]

        # calc uncollected fees
        (
            result["qtty_token0"],
            result["qtty_token1"],
        ) = univ3_formulas.get_uncollected_fees(
            fee_growth_global_0=feeGrowthGlobal0X128,
            fee_growth_global_1=feeGrowthGlobal1X128,
            tick_current=tickCurrent,
            tick_lower=tickLower,
            tick_upper=tickUpper,
            fee_growth_outside_0_lower=ticks_lower["feeGrowthOutside0X128"],
            fee_growth_outside_1_lower=ticks_lower["feeGrowthOutside1X128"],
            fee_growth_outside_0_upper=ticks_upper["feeGrowthOutside0X128"],
            fee_growth_outside_1_upper=ticks_upper["feeGrowthOutside1X128"],
            liquidity=pos["liquidity"],
            fee_growth_inside_last_0=pos["feeGrowthInside0LastX128"],
            fee_growth_inside_last_1=pos["feeGrowthInside1LastX128"],
        )

        # convert to decimal as needed
        if inDecimal:
            # get token decimals
            token0, token1 = await asyncio.gather(self.token0, self.token1)
            decimals_token0, decimals_token1 = await asyncio.gather(
                token0.decimals, token1.decimals
            )

            result["qtty_token0"] = Decimal(result["qtty_token0"]) / Decimal(
                10**decimals_token0
            )
            result["qtty_token1"] = Decimal(result["qtty_token1"]) / Decimal(
                10**decimals_token1
            )

        # return result
        return result.copy()

    async def as_dict(self, convert_bint=False, static_mode: bool = False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): convert big integers to string . Defaults to False.
            static_mode (bool, optional): return only static pool parameters. Defaults to False.

        Returns:
            dict:
        """
        result = await super().as_dict(convert_bint=convert_bint)

        (
            result["fee"],
            result["tickSpacing"],
            result["token0"],
            result["token1"],
            result["protocolFees"],
        ) = await asyncio(
            self.fee,
            self.tickSpacing,
            self.token0.as_dict(convert_bint=convert_bint),
            self.token1.as_dict(convert_bint=convert_bint),
            self.protocolFees,
        )

        # identify pool dex
        result["dex"] = self.identify_dex_name()

        if convert_bint:
            result["tickSpacing"] = str(result["tickSpacing"])
            result["protocolFees"] = [str(i) for i in result["protocolFees"]]

        if not static_mode:
            await self._as_dict_not_static_items(convert_bint, result)
        return result

    async def _as_dict_not_static_items(self, convert_bint, result):
        (
            result["feeGrowthGlobal0X128"],
            result["feeGrowthGlobal1X128"],
            result["liquidity"],
            result["maxLiquidityPerTick"],
            result["slot0"],
        ) = await asyncio.gather(
            self.feeGrowthGlobal0X128,
            self.feeGrowthGlobal1X128,
            self.liquidity,
            self.maxLiquidityPerTick,
            self.slot0,
        )

        if convert_bint:
            result["feeGrowthGlobal0X128"] = str(result["feeGrowthGlobal0X128"])
            result["feeGrowthGlobal1X128"] = str(result["feeGrowthGlobal1X128"])
            result["liquidity"] = str(result["liquidity"])
            result["maxLiquidityPerTick"] = str(result["maxLiquidityPerTick"])
            result["slot0"]["sqrtPriceX96"] = str(result["slot0"]["sqrtPriceX96"])
            result["slot0"]["tick"] = str(result["slot0"]["tick"])
            result["slot0"]["observationIndex"] = str(
                result["slot0"]["observationIndex"]
            )
            result["slot0"]["observationCardinality"] = str(
                result["slot0"]["observationCardinality"]
            )
            result["slot0"]["observationCardinalityNext"] = str(
                result["slot0"]["observationCardinalityNext"]
            )


class algebrav3_dataStorageOperator(web3wrap):
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
        self._abi_filename = abi_filename or "dataStorageOperator"
        self._abi_path = abi_path or "sources/common/abis/algebra/v3"

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

        self._feeConfig = None
        self._window = None

    # TODO: Implement contract functs calculateVolumePerLiquidity, getAverages, getFee, getSingleTimepoint, getTimepoints and timepoints

    async def init_feeConfig(self):
        self._feeConfig = await self._contract.functions.feeConfig().call(
            block_identifier=await self.block
        )

    async def init_window(self):
        self._window = await self._contract.functions.window().call(
            block_identifier=await self.block
        )

    @property
    async def feeConfig(self):
        if not self._feeConfig:
            await self.init_feeConfig()
        return self._feeConfig

    @property
    async def window(self):
        if not self._window:
            await self.init_window()
        return self._window


class algebrav3_pool(web3wrap):
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
        self._abi_filename = abi_filename or "algebrav3pool"
        self._abi_path = abi_path or "sources/common/abis/algebra/v3"

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


        self._activeIncentive = None
        self._dataStorageOperator = None
        self._factory = None
        self._globalState = None
        self._liquidity = None
        self._liquidityCooldown = None
        self._maxLiquidityPerTick = None
        self._tickSpacing = None
        self._token0: erc20 = None
        self._token1: erc20 = None
        self._feeGrowthGlobal0X128 = None
        self._feeGrowthGlobal1X128 = None

    async def init_activeIncentive(self):
        self._activeIncentive = self._contract.functions.activeIncentive().call(
            block_identifier=await self.block
        )

    async def init_dataStorageOperator(self):
        self._dataStorageOperator_address = (
            await self._contract.functions.dataStorageOperator().call(
                block_identifier=await self.block
            )
        )
        self._dataStorageOperator = algebrav3_dataStorageOperator(
            address=self._dataStorageOperator_address,
            network=self._network,
            block=await self.block,
            timestamp=await self.timestamp,
            custom_web3Url=self.w3.provider.endpoint_uri,
        )

    async def init_factory(self):
        self._factory = self._contract.functions.factory().call(
            block_identifier=await self.block
        )

    async def init_globalState(self):
        """

        Returns:
           dict:   sqrtPriceX96  uint160 :  28854610805518743926885543006518067  ( <price> at contract level)
                   tick   int24 :  256121
                   fee   uint16 :  198
                   timepointIndex   uint16 :  300
                   communityFeeToken0   uint8 :  300
                   communityFeeToken1   uint8 :  0
                   unlocked   bool :  true
        """
        tmp = await self._contract.functions.globalState().call(
            block_identifier=await self.block
        )
        self._globalState = {
            "sqrtPriceX96": tmp[0],
            "tick": tmp[1],
            "fee": tmp[2],
            "timepointIndex": tmp[3],
            "communityFeeToken0": tmp[4],
            "communityFeeToken1": tmp[5],
            "unlocked": tmp[6],
        }

    async def init_liquidity(self):
        self._liquidity = self._contract.functions.liquidity().call(
            block_identifier=await self.block
        )

    async def init_liquidityCooldown(self):
        self._liquidityCooldown = self._contract.functions.liquidityCooldown().call(
            block_identifier=await self.block
        )

    async def init_maxLiquidityPerTick(self):
        self._maxLiquidityPerTick = self._contract.functions.maxLiquidityPerTick().call(
            block_identifier=await self.block
        )

    async def init_tickSpacing(self):
        self._tickSpacing = self._contract.functions.tickSpacing().call(
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

    async def init_feeGrowthGlobal0X128(self):
        self._feeGrowthGlobal0X128 = (
            await self._contract.functions.totalFeeGrowth0Token().call(
                block_identifier=await self.block
            )
        )

    async def init_feeGrowthGlobal1X128(self):
        self._feeGrowthGlobal1X128 = (
            await self._contract.functions.totalFeeGrowth1Token().call(
                block_identifier=await self.block
            )
        )

    # Properties
    @property
    async def activeIncentive(self) -> str:
        if not self._activeIncentive:
            await self.init_activeIncentive()
        return self._activeIncentive

    @property
    async def dataStorageOperator(self) -> algebrav3_dataStorageOperator:
        if not self._dataStorageOperator:
            await self.init_dataStorageOperator()
        return self._dataStorageOperator_address

    @property
    async def factory(self) -> str:
        if not self._factory:
            await self.init_factory()
        return self._factory

    @property
    async def globalState(self) -> dict:
        if not self._globalState:
            await self.init_globalState()
        return self._globalState

    @property
    async def liquidity(self) -> int:
        if not self._liquidity:
            await self.init_liquidity()
        return self._liquidity

    @property
    async def liquidityCooldown(self) -> int:
        if not self._liquidityCooldown:
            await self.init_liquidityCooldown()
        return self._liquidityCooldown

    @property
    async def maxLiquidityPerTick(self) -> int:
        if not self._maxLiquidityPerTick:
            await self.init_maxLiquidityPerTick()
        return self._maxLiquidityPerTick

    @property
    async def tickSpacing(self) -> int:
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

    @property
    async def feeGrowthGlobal0X128(self) -> int:
        if not self._feeGrowthGlobal0X128:
            await self.init_feeGrowthGlobal0X128()
        return self._feeGrowthGlobal0X128

    @property
    async def feeGrowthGlobal1X128(self) -> int:
        if not self._feeGrowthGlobal1X128:
            await self.init_feeGrowthGlobal1X128()
        return self._feeGrowthGlobal1X128

    @property
    async def getInnerCumulatives(self, bottomTick: int, topTick: int) -> dict:
        return await self._contract.functions.getInnerCumulatives(
            bottomTick, topTick
        ).call(block_identifier=await self.block)

    @property
    async def getTimepoints(self, secondsAgo: int) -> dict:
        return await self._contract.functions.getTimepoints(secondsAgo).call(
            block_identifier=await self.block
        )

    async def positions(self, position_key: str) -> dict:
        """

        Args:
           position_key (str): 0x....

        Returns:
           _type_:
                   liquidity   uint128 :  99225286851746
                   lastLiquidityAddTimestamp
                   innerFeeGrowth0Token   uint256 :  (feeGrowthInside0LastX128)
                   innerFeeGrowth1Token   uint256 :  (feeGrowthInside1LastX128)
                   fees0   uint128 :  0  (tokensOwed0)
                   fees1   uint128 :  0  ( tokensOwed1)
        """

        result = await self._contract.functions.positions(HexBytes(position_key)).call(
            block_identifier=await self.block
        )
        return {
            "liquidity": result[0],
            "lastLiquidityAddTimestamp": result[1],
            "feeGrowthInside0LastX128": result[2],
            "feeGrowthInside1LastX128": result[3],
            "tokensOwed0": result[4],
            "tokensOwed1": result[5],
        }

    async def tickTable(self, value: int) -> int:
        return await self._contract.functions.tickTable(value).call(
            block_identifier=await self.block
        )

    async def ticks(self, tick: int) -> dict:
        """

        Args:
           tick (int):

        Returns:
           _type_:     liquidityGross   uint128 :  0        liquidityTotal
                       liquidityNet   int128 :  0           liquidityDelta
                       feeGrowthOutside0X128   uint256 :  0 outerFeeGrowth0Token
                       feeGrowthOutside1X128   uint256 :  0 outerFeeGrowth1Token
                       tickCumulativeOutside   int56 :  0   outerTickCumulative
                       spoolecondsPerLiquidityOutsideX128   uint160 :  0    outerSecondsPerLiquidity
                       secondsOutside   uint32 :  0         outerSecondsSpent
                       initialized   bool :  false          initialized
        """
        result = await self._contract.functions.ticks(tick).call(
            block_identifier=await self.block
        )
        return {
            "liquidityGross": result[0],
            "liquidityNet": result[1],
            "feeGrowthOutside0X128": result[2],
            "feeGrowthOutside1X128": result[3],
            "tickCumulativeOutside": result[4],
            "secondsPerLiquidityOutsideX128": result[5],
            "secondsOutside": result[6],
            "initialized": result[7],
        }

    async def timepoints(self, index: int) -> dict:
        #   initialized bool, blockTimestamp uint32, tickCumulative int56, secondsPerLiquidityCumulative uint160, volatilityCumulative uint88, averageTick int24, volumePerLiquidityCumulative uint144
        return await self._contract.functions.timepoints(index).call(
            block_identifier=await self.block
        )

    # CUSTOM PROPERTIES
    @property
    async def block(self) -> int:
        return await super().block

    @block.setter
    def block(self, value: int):
        # set block
        self._block = value
        if self._token0:
            self._token0.block = value
        if self._token1:
            self._token1.block = value

    @property
    async def timestamp(self) -> int:
        """ """
        return await super().timestamp

    @timestamp.setter
    def timestamp(self, value: int):
        self._timestamp = value
        if self._token0:
            self._token0.timestamp = value
        if self._token1:
            self._token1.timestamp = value

    # CUSTOM FUNCTIONS
    async def position(self, ownerAddress: str, tickLower: int, tickUpper: int) -> dict:
        """

        Returns:
           dict:
                   liquidity   uint128 :  99225286851746
                   feeGrowthInside0LastX128   uint256 :  0
                   feeGrowthInside1LastX128   uint256 :  0
                   tokensOwed0   uint128 :  0
                   tokensOwed1   uint128 :  0
        """
        return await self.positions(
            univ3_formulas.get_positionKey_algebra(
                ownerAddress=ownerAddress,
                tickLower=tickLower,
                tickUpper=tickUpper,
            )
        )

    async def get_qtty_depoloyed(
        self, ownerAddress: str, tickUpper: int, tickLower: int, inDecimal: bool = True
    ) -> dict:
        """Retrieve the quantity of tokens currently deployed

        Args:
           ownerAddress (str):
           tickUpper (int):
           tickLower (int):
           inDecimal (bool): return result in a decimal format?

        Returns:
           dict: {
                   "qtty_token0":0,        (int or Decimal) # quantity of token 0 deployed in dex
                   "qtty_token1":0,        (int or Decimal) # quantity of token 1 deployed in dex
                   "fees_owed_token0":0,   (int or Decimal) # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
                   "fees_owed_token1":0,   (int or Decimal) # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
                 }
        """

        result = {
            "qtty_token0": 0,  # quantity of token 0 deployed in dex
            "qtty_token1": 0,  # quantity of token 1 deployed in dex
            "fees_owed_token0": 0,  # quantity of token 0 fees owed to the position ( not included in qtty_token0 and this is not uncollected fees)
            "fees_owed_token1": 0,  # quantity of token 1 fees owed to the position ( not included in qtty_token1 and this is not uncollected fees)
        }

        # get position and slot data
        pos = await self.position(
            ownerAddress=Web3.to_checksum_address(ownerAddress.lower()),
            tickLower=tickLower,
            tickUpper=tickUpper,
        )

        slot0 = self.globalState

        # get current tick from slot
        tickCurrent = slot0["tick"]
        sqrtRatioX96 = slot0["sqrtPriceX96"]
        sqrtRatioAX96 = univ3_formulas.TickMath.getSqrtRatioAtTick(tickLower)
        sqrtRatioBX96 = univ3_formulas.TickMath.getSqrtRatioAtTick(tickUpper)
        # calc quantity from liquidity
        (
            result["qtty_token0"],
            result["qtty_token1"],
        ) = univ3_formulas.LiquidityAmounts.getAmountsForLiquidity(
            sqrtRatioX96, sqrtRatioAX96, sqrtRatioBX96, pos["liquidity"]
        )

        # add owed tokens
        result["fees_owed_token0"] = pos["tokensOwed0"]
        result["fees_owed_token1"] = pos["tokensOwed1"]

        # convert to decimal as needed
        if inDecimal:
            self._get_qtty_depoloyed_todecimal(result)
        # return result
        return result.copy()

    def _get_qtty_depoloyed_todecimal(self, result):
        # get token decimals
        decimals_token0 = self.token0.decimals
        decimals_token1 = self.token1.decimals

        result["qtty_token0"] = Decimal(result["qtty_token0"]) / Decimal(
            10**decimals_token0
        )
        result["qtty_token1"] = Decimal(result["qtty_token1"]) / Decimal(
            10**decimals_token1
        )
        result["fees_owed_token0"] = Decimal(result["fees_owed_token0"]) / Decimal(
            10**decimals_token0
        )
        result["fees_owed_token1"] = Decimal(result["fees_owed_token1"]) / Decimal(
            10**decimals_token1
        )

    async def get_fees_uncollected(
        self, ownerAddress: str, tickUpper: int, tickLower: int, inDecimal: bool = True
    ) -> dict:
        """Retrieve the quantity of fees not collected nor yet owed ( but certain) to the deployed position

        Args:
            ownerAddress (str):
            tickUpper (int):
            tickLower (int):
            inDecimal (bool): return result in a decimal format?

        Returns:
            dict: {
                    "qtty_token0":0,   (int or Decimal)     # quantity of uncollected token 0
                    "qtty_token1":0,   (int or Decimal)     # quantity of uncollected token 1
                }
        """

        result = {
            "qtty_token0": 0,
            "qtty_token1": 0,
            "qtty_token0_owed": 0,
            "qtty_token1_owed": 0,
        }

        # get position and ticks data
        (
            pos,
            ticks_lower,
            ticks_upper,
            globalState,
            feeGrowthGlobal0X128,
            feeGrowthGlobal1X128,
        ) = await asyncio.gather(
            self.position(
                ownerAddress=Web3.to_checksum_address(ownerAddress.lower()),
                tickLower=tickLower,
                tickUpper=tickUpper,
            ),
            self.ticks(tickLower),
            self.ticks(tickUpper),
            self.globalState,
            self.feeGrowthGlobal0X128,
            self.feeGrowthGlobal1X128,
        )

        tickCurrent = globalState["tick"]

        # get fees owed
        result["qtty_token0_owed"] = pos["tokensOwed0"]
        result["qtty_token1_owed"] = pos["tokensOwed1"]

        # calc uncollected fees
        (
            result["qtty_token0"],
            result["qtty_token1"],
        ) = univ3_formulas.get_uncollected_fees(
            fee_growth_global_0=feeGrowthGlobal0X128,
            fee_growth_global_1=feeGrowthGlobal1X128,
            tick_current=tickCurrent,
            tick_lower=tickLower,
            tick_upper=tickUpper,
            fee_growth_outside_0_lower=ticks_lower["feeGrowthOutside0X128"],
            fee_growth_outside_1_lower=ticks_lower["feeGrowthOutside1X128"],
            fee_growth_outside_0_upper=ticks_upper["feeGrowthOutside0X128"],
            fee_growth_outside_1_upper=ticks_upper["feeGrowthOutside1X128"],
            liquidity=pos["liquidity"],
            fee_growth_inside_last_0=pos["feeGrowthInside0LastX128"],
            fee_growth_inside_last_1=pos["feeGrowthInside1LastX128"],
        )

        # convert to decimal as needed
        if inDecimal:
            # get token decimals
            token0, token1 = await asyncio.gather(self.token0, self.token1)
            decimals_token0, decimals_token1 = await asyncio.gather(
                token0.decimals, token1.decimals
            )

            result["qtty_token0"] = Decimal(result["qtty_token0"]) / Decimal(
                10**decimals_token0
            )
            result["qtty_token1"] = Decimal(result["qtty_token1"]) / Decimal(
                10**decimals_token1
            )

        # return result
        return result.copy()

    async def as_dict(self, convert_bint=False, static_mode: bool = False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): convert big integers to string. Defaults to False.
            static_mode (bool, optional): return  static fields only. Defaults to False.

        Returns:
            dict:
        """

        result = await super().as_dict(convert_bint=convert_bint)

        (
            result["activeIncentive"],
            result["liquidityCooldown"],
            result["maxLiquidityPerTick"],
            result["token0"],
            result["token1"],
            result["globalState"],
        ) = await asyncio.gather(
            self.activeIncentive,
            self.liquidityCooldown,
            self.maxLiquidityPerTick,
            self.token0.as_dict(convert_bint=convert_bint),
            self.token1.as_dict(convert_bint=convert_bint),
            self.globalState,
        )

        result["fee"] = (result["globalState"]["fee"],)

        if convert_bint:
            result["liquidityCooldown"] = str(result["liquidityCooldown"])
            result["maxLiquidityPerTick"] = str(result["maxLiquidityPerTick"])

        # t spacing
        # result["tickSpacing"]

        # identify pool dex
        result["dex"] = self.identify_dex_name()

        if not static_mode:
            (
                result["feeGrowthGlobal0X128"],
                result["feeGrowthGlobal1X128"],
                result["liquidity"],
            ) = await asyncio.gather(
                self.feeGrowthGlobal0X128,
                self.feeGrowthGlobal1X128,
                self.liquidity,
            )

            if convert_bint:
                result["feeGrowthGlobal0X128"] = str(result["feeGrowthGlobal0X128"])
                result["feeGrowthGlobal1X128"] = str(result["feeGrowthGlobal1X128"])
                result["liquidity"] = str(result["liquidity"])

                try:
                    result["globalState"]["sqrtPriceX96"] = (
                        str(result["globalState"]["sqrtPriceX96"])
                        if "sqrtPriceX96" in result["globalState"]
                        else ""
                    )
                    # result["globalState"]["price"] = (
                    #     str(result["globalState"]["price"])
                    #     if "price" in result["globalState"]
                    #     else ""
                    # )
                    result["globalState"]["tick"] = (
                        str(result["globalState"]["tick"])
                        if "tick" in result["globalState"]
                        else ""
                    )
                    result["globalState"]["fee"] = (
                        str(result["globalState"]["fee"])
                        if "fee" in result["globalState"]
                        else ""
                    )
                    result["globalState"]["timepointIndex"] = (
                        str(result["globalState"]["timepointIndex"])
                        if "timepointIndex" in result["globalState"]
                        else ""
                    )
                except Exception:
                    logging.getLogger(__name__).warning(
                        f' Unexpected error converting globalState of {result["address"]} at block {result["block"]}     error-> {sys.exc_info()[0]}   globalState: {result["globalState"]}'
                    )

        return result
