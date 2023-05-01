import logging
import math
import datetime as dt

from web3 import Web3, exceptions, AsyncWeb3, AsyncHTTPProvider
from web3.eth import AsyncEth
from web3.net import AsyncNet
from web3.contract import Contract
from web3.middleware import async_geth_poa_middleware, async_simple_cache_middleware

import asyncio

from sources.web3.bins.configuration import CONFIGURATION
from sources.web3.bins.general import file_utilities


class web3wrap:
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
        # set init vars
        self._address = Web3.to_checksum_address(address)
        self._network = network
        # progress
        self._progress_callback = None

        # set optionals
        self.setup_abi(abi_filename=abi_filename, abi_path=abi_path)

        # setup Web3
        self._w3 = custom_web3 or self.setup_w3(
            network=self._network, web3Url=custom_web3Url
        )

        # setup contract to query
        self.setup_contract(contract_address=self._address, contract_abi=self._abi)

        self._timestamp = timestamp
        self._block = block
        self._block_data = None

    # initializers
    async def init_block(self) -> tuple[int, int]:
        self._block_data = await self._w3.eth.get_block(self._block or "latest")
        self.block = self._block_data.number
        self.timestamp = self._block_data.timestamp

        return self._block, self._timestamp

    def setup_abi(self, abi_filename: str, abi_path: str):
        # set optionals
        if abi_filename != "":
            self._abi_filename = abi_filename
        if abi_path != "":
            self._abi_path = abi_path
        # load abi
        self._abi = file_utilities.load_json(
            filename=self._abi_filename, folder_path=self._abi_path
        )

    def setup_w3(self, network: str, web3Url: str | None = None) -> Web3:
        # setup web3
        result = AsyncWeb3(
            AsyncHTTPProvider(
                web3Url or CONFIGURATION["sources"]["web3Providers"][network],
                request_kwargs={"timeout": 120},
            ),
            modules={"eth": AsyncEth, "net": AsyncNet},
        )

        # add simple cache module
        result.middleware_onion.add(async_simple_cache_middleware)

        # add middleware as needed
        if network != "ethereum":
            result.middleware_onion.inject(async_geth_poa_middleware, layer=0)

        return result

    def setup_contract(self, contract_address: str, contract_abi: str):
        # set contract
        self._contract = self._w3.eth.contract(
            address=contract_address, abi=contract_abi
        )

    # CUSTOM PROPERTIES
    @property
    def address(self) -> str:
        return self._address

    @property
    def w3(self) -> Web3:
        return self._w3

    @property
    def contract(self) -> Contract:
        return self._contract

    @property
    async def block(self) -> int:
        """ """
        if not self._block:
            await self.init_block()

        return self._block

    @block.setter
    def block(self, value: int):
        self._block = value

    @property
    async def timestamp(self) -> int:
        """ """
        if not self._timestamp:
            await self.init_block()
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value: int):
        self._timestamp = value

    # HELPERS
    async def average_blockTime(self, blocksaway: int = 500) -> dt.datetime.timestamp:
        """Average time of block creation

        Args:
           blocksaway (int, optional): blocks used compute average. Defaults to 500.

        Returns:
           dt.datetime.timestamp: average time per block
        """
        result: int = 0
        # no decimals allowed
        blocksaway: int = math.floor(blocksaway)
        #
        if blocksaway > 0:
            block_current, block_past = await asyncio.gather(
                self._w3.eth.get_block("latest"),
                self._w3.eth.get_block(block_current.number - blocksaway),
            )
            result: int = (block_current.timestamp - block_past.timestamp) / blocksaway
        return result

    async def blockNumberFromTimestamp(
        self,
        timestamp: dt.datetime.timestamp,
        inexact_mode="before",
        eq_timestamp_position="first",
    ) -> int:
        """Will
           At least 15 queries are needed to come close to a timestamp block number

        Args:
           timestamp (dt.datetime.timestamp): _description_
           inexact_mode (str): "before" or "after" -> if found closest to timestapm, choose a block before of after objective
           eq_timestamp_position (str): first or last position to choose when a timestamp corresponds to multiple blocks ( so choose the first or the last one of those blocks)

        Returns:
           int: blocknumber
        """

        if int(timestamp) == 0:
            raise ValueError("Timestamp cannot be zero!")

        # check min timestamp
        min_block = await self._w3.eth.get_block(1)
        if min_block.timestamp > timestamp:
            return 1

        queries_cost = 0
        found_exact = False

        block_curr = await self._w3.eth.get_block("latest")
        first_step = math.ceil(block_curr.number * 0.85)

        # make sure we have positive block result
        while (block_curr.number + first_step) <= 0:
            first_step -= 1
        # calc blocks to go up/down closer to goal
        block_past = await self._w3.eth.get_block(block_curr.number - (first_step))
        blocks_x_timestamp = (
            abs(block_curr.timestamp - block_past.timestamp) / first_step
        )

        block_step = (block_curr.timestamp - timestamp) / blocks_x_timestamp
        block_step_sign = -1

        _startime = dt.datetime.now(dt.timezone.utc)

        while block_curr.timestamp != timestamp:
            queries_cost += 1

            # make sure we have positive block result
            while (block_curr.number + (block_step * block_step_sign)) <= 0:
                if queries_cost != 1:
                    # change sign and lower steps
                    block_step_sign *= -1
                # first time here, set lower block steps
                block_step /= 2
            # go to block
            try:
                block_curr = await self._w3.eth.get_block(
                    math.floor(block_curr.number + (block_step * block_step_sign))
                )
            except exceptions.BlockNotFound:
                # diminish step
                block_step /= 2
                continue

            blocks_x_timestamp = (
                (
                    abs(block_curr.timestamp - block_past.timestamp)
                    / abs(block_curr.number - block_past.number)
                )
                if abs(block_curr.number - block_past.number) != 0
                else 0
            )
            if blocks_x_timestamp != 0:
                block_step = math.ceil(
                    abs(block_curr.timestamp - timestamp) / blocks_x_timestamp
                )

            if block_curr.timestamp < timestamp:
                # block should be higher than current
                block_step_sign = 1
            elif block_curr.timestamp > timestamp:
                # block should be lower than current
                block_step_sign = -1
            else:
                # got it
                found_exact = True
                # exit loop
                break

            # set block past
            block_past = block_curr

            # 15sec while loop safe exit (an eternity to find the block)
            if (dt.datetime.now(dt.timezone.utc) - _startime).total_seconds() > 15:
                if inexact_mode == "before":
                    # select block smaller than objective
                    while block_curr.timestamp > timestamp:
                        block_curr = await self._w3.eth.get_block(block_curr.number - 1)
                elif inexact_mode == "after":
                    # select block greater than objective
                    while block_curr.timestamp < timestamp:
                        block_curr = await self._w3.eth.get_block(block_curr.number + 1)
                else:
                    raise ValueError(
                        f" Inexact method chosen is not valid:->  {inexact_mode}"
                    )
                # exit loop
                break

        # define result
        result = block_curr.number

        # get blocks with same timestamp
        sametimestampBlocks = await self.get_sameTimestampBlocks(
            block_curr, queries_cost
        )
        if len(sametimestampBlocks) > 0:
            if eq_timestamp_position == "first":
                result = sametimestampBlocks[0]
            elif eq_timestamp_position == "last":
                result = sametimestampBlocks[-1]

        # log result
        if found_exact:
            logging.getLogger(__name__).debug(
                f" Took {queries_cost} on-chain queries to find block number {block_curr.number} of timestamp {timestamp}"
            )

        else:
            logging.getLogger(__name__).warning(
                f" Could not find the exact block number from timestamp -> took {queries_cost} on-chain queries to find block number {block_curr.number} ({block_curr.timestamp}) closest to timestamp {timestamp}  -> original-found difference {timestamp - block_curr.timestamp}"
            )

        # return closest block found
        return result

    async def timestampFromBlockNumber(self, block: int) -> int:
        block_obj = None
        if block < 1:
            block_obj = await self._w3.eth.get_block("latest")
        else:
            block_obj = await self._w3.eth.get_block(block)

        # return closest block found
        return block_obj.timestamp

    async def get_sameTimestampBlocks(self, block, queries_cost: int):
        result = []
        # try go backwards till different timestamp is found
        curr_block = block
        while curr_block.timestamp == block.timestamp:
            if curr_block.number != block.number:
                result.append(curr_block.number)
            curr_block = await self._w3.eth.get_block(curr_block.number - 1)
            queries_cost += 1
        # try go forward till different timestamp is found
        curr_block = block
        while curr_block.timestamp == block.timestamp:
            if curr_block.number != block.number:
                result.append(curr_block.number)
            curr_block = await self._w3.eth.get_block(curr_block.number + 1)
            queries_cost += 1

        return sorted(result)

    def create_eventFilter_chunks(self, eventfilter: dict, max_blocks=1000) -> list:
        """create a list of event filters
           to be able not to timeout servers

        Args:
           eventfilter (dict):  {'fromBlock': ,
                                   'toBlock': block,
                                   'address': [self._address],
                                   'topics': [self._topics[operation]],
                                   }

        Returns:
           list: of the same
        """
        result = []
        tmp_filter = dict(eventfilter)
        toBlock = eventfilter["toBlock"]
        fromBlock = eventfilter["fromBlock"]
        blocksXfilter = math.ceil((toBlock - fromBlock) / max_blocks)

        current_fromBlock = tmp_filter["fromBlock"]
        current_toBlock = current_fromBlock + max_blocks
        for _ in range(blocksXfilter):
            # mod filter blocks
            tmp_filter["toBlock"] = current_toBlock
            tmp_filter["fromBlock"] = current_fromBlock

            # append filter
            result.append(dict(tmp_filter))

            # exit if done...
            if current_toBlock == toBlock:
                break

            # increment chunk
            current_fromBlock = current_toBlock + 1
            current_toBlock = current_fromBlock + max_blocks
            if current_toBlock > toBlock:
                current_toBlock = toBlock

        # return result
        return result

    async def get_chunked_events(self, eventfilter, max_blocks=2000):
        result = []
        # get a list of filters with different block chunks
        for _filter in await self.create_eventFilter_chunks(
            eventfilter=eventfilter, max_blocks=max_blocks
        ):
            entries = await self._w3.eth.filter(_filter).get_all_entries()

            # progress if no data found
            if self._progress_callback and len(entries) == 0:
                self._progress_callback(
                    text=f'no matches from blocks {_filter["fromBlock"]} to {_filter["toBlock"]}',
                    remaining=eventfilter["toBlock"] - _filter["toBlock"],
                    total=eventfilter["toBlock"] - eventfilter["fromBlock"],
                )
            result.append(entries)

            # filter blockchain data
        return result

    def identify_dex_name(self) -> str:
        """Return dex name using the calling object's type

        Returns:
            str: "uniswapv3", "quickswap" or  not Implemented error
        """
        # cross reference import
        from sources.web3.bins.w3.objects.protocols import (
            gamma_hypervisor,
            gamma_hypervisor_quickswap,
            gamma_hypervisor_zyberswap,
            gamma_hypervisor_thena,
        )
        from sources.web3.bins.w3.objects.exchanges import univ3_pool, algebrav3_pool

        #######################

        if isinstance(self, univ3_pool) or issubclass(type(self), univ3_pool):
            return "uniswapv3"

        elif isinstance(self, algebrav3_pool) or issubclass(type(self), algebrav3_pool):
            return "algebrav3"

        elif isinstance(self, (gamma_hypervisor_quickswap)) or issubclass(
            type(self), (gamma_hypervisor_quickswap)
        ):
            return "quickswap"

        elif isinstance(self, (gamma_hypervisor_zyberswap)) or issubclass(
            type(self), (gamma_hypervisor_zyberswap)
        ):
            return "zyberswap"
        elif isinstance(self, (gamma_hypervisor_thena)) or issubclass(
            type(self), (gamma_hypervisor_thena)
        ):
            return "thena"

        # KEEP GAMMA AT THE BOTTOM
        elif isinstance(self, gamma_hypervisor) or issubclass(
            type(self), gamma_hypervisor
        ):
            return "uniswapv3"

        else:
            raise NotImplementedError(
                f" Dex name cannot be identified using object type {type(self)}"
            )

    async def as_dict(self, convert_bint=False) -> dict:
        # it will only fill when block is not set

        result = {}
        result["block"], result["timestamp"] = await asyncio.gather(
            self.block, self.timestamp
        )
        # lower case address to be able to be directly compared
        result["address"] = self.address.lower()
        return result


class erc20(web3wrap):
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
        self._abi_filename = abi_filename or "erc20"
        self._abi_path = abi_path or "sources/common/abis"

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

        self._decimals = None
        self._totalSupply = None
        self._symbol = None

    async def init_decimals(self):
        self._decimals = await self._contract.functions.decimals().call()

    async def init_totalSupply(self):
        self._totalSupply = await self._contract.functions.totalSupply().call(
            block_identifier=await self.block
        )

    async def init_symbol(self):
        self._symbol = (
            "MKR"
            if self.address == "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2"
            else await self._contract.functions.symbol().call()
        )

    # PROPERTIES

    @property
    async def decimals(self) -> int:
        if not self._decimals:
            await self.init_decimals()
        return self._decimals

    @property
    async def totalSupply(self) -> int:
        if not self._totalSupply:
            await self.init_totalSupply()
        return self._totalSupply

    @property
    async def symbol(self) -> str:
        if not self._symbol:
            await self.init_symbol()
        return self._symbol

    async def balanceOf(self, address: str) -> int:
        return await self._contract.functions.balanceOf(
            Web3.to_checksum_address(address)
        ).call(block_identifier=await self.block)

    async def allowance(self, owner: str, spender: str) -> int:
        return await self._contract.functions.allowance(
            Web3.to_checksum_address(owner), Web3.to_checksum_address(spender)
        ).call(block_identifier=await self.block)

    async def as_dict(self, convert_bint=False) -> dict:
        """as_dict _summary_

        Args:
            convert_bint (bool, optional): Convert big integers to strings ? . Defaults to False.

        Returns:
            dict: decimals, totalSupply(bint) and symbol dict
        """
        result = await super().as_dict(convert_bint=convert_bint)

        (
            result["decimals"],
            result["totalSupply"],
            result["symbol"],
        ) = await asyncio.gather(self.decimals, self.totalSupply, self.symbol)

        return result
