from dataclasses import dataclass
from decimal import Decimal
from typing import MutableMapping
import pandas as pd
from datetime import datetime, timezone
from sources.common.general.enums import Chain
from sources.common.general.utils import convert_to_csv, flatten_dict


@dataclass
class time_location:
    timestamp: int = None
    block: int = None

    @property
    def datetime(self) -> datetime:
        """UTC datetime from timestamp
        Can return None when timestamp is None"""
        return (
            datetime.fromtimestamp(self.timestamp, tz=timezone.utc)
            if self.timestamp
            else None
        )

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "block": self.block,
        }


@dataclass
class period_timeframe:
    ini: time_location = None
    end: time_location = None

    @property
    def seconds(self) -> int:
        return self.end.timestamp - self.ini.timestamp

    @property
    def blocks(self) -> int:
        return self.end.block - self.ini.block

    def to_dict(self) -> dict:
        return {
            "ini": self.ini.to_dict(),
            "end": self.end.to_dict(),
            "seconds": self.seconds,
            "blocks": self.blocks,
        }

    def from_dict(self, item: dict):
        self.ini = time_location(
            timestamp=item["ini"]["timestamp"], block=item["ini"]["block"]
        )
        self.end = time_location(
            timestamp=item["end"]["timestamp"], block=item["end"]["block"]
        )


@dataclass
class token_group:
    token0: Decimal = None
    token1: Decimal = None

    def to_dict(self) -> dict:
        return {
            "token0": self.token0,
            "token1": self.token1,
        }

    def from_dict(self, item: dict):
        self.token0 = item["token0"]
        self.token1 = item["token1"]


@dataclass
class underlying_value:
    qtty: token_group = None
    details: dict = None

    def to_dict(self) -> dict:
        return {
            "qtty": self.qtty.to_dict(),
            "details": self.details or {},
        }

    def from_dict(self, item: dict):
        self.qtty = token_group()
        self.qtty.from_dict(item["qtty"])
        self.details = item["details"]


@dataclass
class qtty_usd_yield:
    qtty: token_group = None
    period_yield: Decimal = None

    def to_dict(self) -> dict:
        result = {}
        if self.qtty:
            result["qtty"] = self.qtty.to_dict()
        result["period_yield"] = self.period_yield
        return result

    def from_dict(self, item: dict):
        self.qtty = token_group()
        self.qtty.from_dict(item["qtty"])
        self.period_yield = item["period_yield"]


@dataclass
class rewards_group:
    usd: Decimal = None
    period_yield: Decimal = None
    details: list = None

    def to_dict(self) -> dict:
        return {
            "usd": self.usd,
            "period_yield": self.period_yield,
            "details": self.details or [],
        }

    def from_dict(self, item: dict):
        self.usd = item["usd"]
        self.period_yield = item["period_yield"]
        self.details = item["details"]


@dataclass
class status_group:
    prices: token_group = None
    underlying: underlying_value = None
    supply: Decimal = None

    def to_dict(self) -> dict:
        return {
            "prices": self.prices.to_dict(),
            "underlying": self.underlying.to_dict(),
            "supply": self.supply,
        }

    def from_dict(self, item: dict):
        self.prices = token_group()
        self.prices.from_dict(item["prices"])
        self.underlying = underlying_value()
        self.underlying.from_dict(item["underlying"])
        self.supply = item["supply"]


@dataclass
class period_status:
    ini: status_group = None
    end: status_group = None

    @property
    def supply_difference(self) -> Decimal:
        """Returns the difference in supply between end and ini

        Returns:
            Decimal:
        """
        return self.end.supply - self.ini.supply

    def to_dict(self) -> dict:
        return {
            "ini": self.ini.to_dict(),
            "end": self.end.to_dict(),
        }

    def from_dict(self, item: dict):
        self.ini = status_group()
        self.ini.from_dict(item["ini"])
        self.end = status_group()
        self.end.from_dict(item["end"])


## READ ONLY PERIOD YIELD DATA OBJECT ##
# minimum functionality to be loaded from the database, basically
@dataclass
class period_yield_data:
    """This class contains all the data needed to calculate the yield of a period
    for a given hypervisor.

        # The 'returns' object is a calculation of fees, rewards and divergence yield over a period of time
        # comprehended between two consecutive hypervisor operations affecting its composition ( one of rebalance, zeroBurn, deposit, withdraw).
        # So, when a 'rebalance/...' happens, the return period starts and when the next 'rebalance/...' happen, the return period ends at that block -1
    """

    # hypervisor address
    address: str = None

    # period timeframe
    timeframe: period_timeframe = None

    # initial and end hypervisor snapshots
    status: period_status = None

    # fees collected during the period ( LPing ) using uncollected fees.
    # this is LPs fees ( not including gamma fees )
    fees: qtty_usd_yield = None
    fees_gamma: qtty_usd_yield = None

    # fees collected by the pool during the period ( calculated using fees+fees_gamma and pool.fee).
    # gross_fees: qtty_usd_yield = None

    # rewards collected during the period ( LPing ) using uncollected fees. This is not accurate when rewards do not include the "extra" info ( absolute qtty of rewards)
    rewards: rewards_group = None

    # fees collected right during block creation, reseting uncollected fees to zero. This is not used for yield calculation, but useful for analysis.
    fees_collected_within: qtty_usd_yield = None

    # Divergence Loss/Gain due to rebalance between hypervisor periods: ( hypervisor period 0 end block - hypervisor period 1 ini block )
    rebalance_divergence: token_group = None

    @property
    def id(self) -> str:
        return f"{self.address}_{self.timeframe.ini.block}_{self.timeframe.end.block}"

    @property
    def period_blocks_qtty(self) -> int:
        return self.timeframe.blocks

    @property
    def period_seconds(self) -> int:
        return self.timeframe.seconds

    @property
    def period_days(self) -> float:
        return self.period_seconds / (24 * 60 * 60)

    @property
    def ini_underlying_usd(self) -> float:
        t0 = t1 = 0
        try:
            t0 = self.status.ini.underlying.qtty.token0 * self.status.ini.prices.token0
        except:
            pass
        try:
            t1 = self.status.ini.underlying.qtty.token1 * self.status.ini.prices.token1
        except:
            pass
        return t0 + t1

    @property
    def end_underlying_usd(self) -> float:
        t0 = t1 = 0
        try:
            t0 = self.status.end.underlying.qtty.token0 * self.status.end.prices.token0
        except:
            pass
        try:
            t1 = self.status.end.underlying.qtty.token1 * self.status.end.prices.token1
        except:
            pass
        return t0 + t1

    # LP FEES
    @property
    def period_fees_usd(self) -> float:
        """fees aquired during the period ( LPing ) using uncollected fees
            (using end prices)

        Returns:
            float:
        """
        t0 = t1 = 0
        try:
            t0 = self.fees.qtty.token0 * self.status.end.prices.token0
        except:
            pass
        try:
            t1 = self.fees.qtty.token1 * self.status.end.prices.token1
        except:
            pass
        return t0 + t1

    @property
    def period_divergence_usd(self) -> float:
        """divergence represents the value change in market prices and pool token weights

        Returns:
            float:
        """
        return self.period_divergence_token0_usd + self.period_divergence_token1_usd

    @property
    def period_divergence_token0(self) -> float:
        """divergence represents the value change in market prices and pool token weights

        Returns:
            float:
        """
        try:
            return (
                self.status.end.underlying.qtty.token0
                - self.status.ini.underlying.qtty.token0
                - self.fees.qtty.token0
            )
        except:
            return Decimal("0")

    @property
    def period_divergence_token0_usd(self) -> float:
        """divergence token0 divergence represents the value change in market prices and pool token weights, converted to usd using end prices
            including rebalance divergence
        Returns:
            float:
        """
        return (self.period_divergence_token0) * self.status.end.prices.token0

    @property
    def period_divergence_token1(self) -> float:
        """divergence divergence represents the value change in market prices and pool token weights

        Returns:
            float:
        """
        try:
            return (
                self.status.end.underlying.qtty.token1
                - self.status.ini.underlying.qtty.token1
                - self.fees.qtty.token1
            )
        except:
            return Decimal("0")

    @property
    def period_divergence_token1_usd(self) -> float:
        """divergence token1 divergence represents the value change in market prices and pool token weights, converted to usd using end prices
            including rebalance divergence
        Returns:
            float:
        """
        return (self.period_divergence_token1) * self.status.end.prices.token1

    @property
    def period_divergence_percentage_yield(self) -> float:
        """divergence divergence represents the value change in market prices and pool token weights

        Returns:
            float: _description_
        """
        return (
            self.period_divergence_usd / self.ini_underlying_usd
            if self.ini_underlying_usd
            else 0
        )

    # price change
    @property
    def period_price_change_token0(self) -> float:
        """end price usd / ini price usd

        Returns:
            float:
        """
        return (
            (self.status.end.prices.token0 - self.status.ini.prices.token0)
            / self.status.ini.prices.token0
            if self.status.ini.prices.token0
            else 0
        )

    @property
    def period_price_change_token1(self) -> float:
        """end price usd / ini price usd

        Returns:
            float:
        """
        return (
            (self.status.end.prices.token1 - self.status.ini.prices.token1)
            / self.status.ini.prices.token1
            if self.status.ini.prices.token1
            else 0
        )

    @property
    def period_price_change_usd(self) -> float:
        """end price usd / ini price usd

        Returns:
            float:
        """
        return (
            (
                self.status.end.prices.token0
                + self.status.end.prices.token1
                - self.status.ini.prices.token0
                - self.status.ini.prices.token1
            )
            / (self.status.ini.prices.token0 + self.status.ini.prices.token1)
            if (self.status.ini.prices.token0 + self.status.ini.prices.token1) > 0
            else 0
        )

    @property
    def price_per_share(self) -> float:
        """Returns the price per share at the end of the period

        Returns:
            float:
        """
        return (
            self.end_underlying_usd / self.status.end.supply
            if self.status.end.supply
            else 0
        )

    @property
    def price_per_share_at_ini(self) -> float:
        """Returns the price per share at the ini of the period

        Returns:
            float:
        """
        return (
            self.ini_underlying_usd / self.status.ini.supply
            if self.status.ini.supply
            else 0
        )

    @property
    def fees_per_share(self) -> float:
        """Return the fees_per_share collected during the period

        Returns:
            float:
        """
        return (
            self.period_fees_usd / self.status.end.supply
            if self.status.end.supply
            else 0
        )

    @property
    def fees_per_share_percentage_yield(self) -> float:
        """Return the fees_per_share collected during the period
            ( as a percentage of the price per share at the beginning of the period)

        Returns:
            float:
        """
        try:
            return self.fees_per_share / self.price_per_share_at_ini
        except:
            pass
        return 0

    @property
    def divergence_per_share(self) -> float:
        """Return the difference between the price per share at the end of the period and the price per share at the beginning of the period
        and subtract the fees_per_share collected during the period
        """
        return (
            (
                self.end_underlying_usd / self.status.end.supply
                if self.status.end.supply
                else 0
            )
            - (
                self.ini_underlying_usd / self.status.ini.supply
                if self.status.ini.supply
                else 0
            )
            - self.fees_per_share
        )

    @property
    def divergence_per_share_percentage_yield(self) -> float:
        """Return the difference between the price per share at the end of the period and the price per share at the beginning of the period
        and subtract the fees_per_share collected during the period
        """
        try:
            return self.divergence_per_share / self.price_per_share_at_ini
        except:
            pass
        return 0

    @property
    def rewards_per_share(self) -> float:
        """Return the rewards_per_share collected during the period

        Returns:
            float:
        """
        try:
            return self.rewards.usd / self.status.end.supply
        except:
            return 0

    @property
    def rewards_per_share_percentage_yield(self) -> float:
        """Return the rewards_per_share collected during the period

        Returns:
            float:
        """
        try:
            return self.rewards.period_yield / self.price_per_share_at_ini
        except:
            return 0

    # CONVERTER
    def to_dict(self) -> dict:
        """convert this object to a dictionary
        Returns:
        {
            "id": ,
            "address":,
            "timeframe": {
                "ini": {
                    "timestamp": ,
                    "block": ,
                    },
                "end": {
                    "timestamp": ,
                    "block": ,
                    },
                "seconds": ,
                "blocks": ,
            },
            "status": {
                "ini": {
                    "prices": {
                        "token0": "",
                        "token1": "",
                    },
                    "underlying": {
                        "qtty": {
                            "token0": "",
                            "token1": "",
                        },
                        "details": {},
                        "usd": "",
                    },
                    "supply": "",
                },
                "end": {
                    "prices": {
                        "token0": "",
                        "token1": "",
                    },
                    "underlying_value": {
                        "qtty": {
                            "token0": "",
                            "token1": "",
                        },
                        "usd": "",
                    },
                    "supply": "",
                },
            },
            "fees": {
                "collected":{
                    "protocol": {
                        "qtty": {
                            "token0": "",
                            "token1": "",
                        },
                        "usd": "",
                        "period_yield": "",
                    }
                    "lps": {
                        "qtty": {
                            "token0": "",
                            "token1": "",
                        },
                        "usd": "",
                        "period_yield": "",
                    }
                }
                "uncollected":{
                    "protocol": {}
                    "lps": {}
                }

            },
            "rewards": {
                "usd": "",
                "period_yield": "",
                "details": [],
            },
            "divergence": {
                "usd": "",
                "percentage_yield": "",
                "qtty": {
                    "token0": "",
                    "token1": "",
                },
                rebalance_divergence: {
                    "token0": "",
                    "token1": "",
                },
            },

            "pool":{
                "gamma_gross_
                    "calculated_gamma_volume_usd": "",
                }

        }
        """
        result = {
            "id": self.id,
            "address": self.address,
            "timeframe": self.timeframe.to_dict(),
            "status": self.status.to_dict(),
            "fees": self.fees.to_dict(),
            "fees_gamma": self.fees_gamma.to_dict(),
            "rewards": self.rewards.to_dict(),
            "fees_collected_within": self.fees_collected_within.to_dict(),
        }

        return result

    def from_dict(self, item: dict):
        """fill this object data from a dictionary

        Args:
            item (dict): dictionary with the data
        """
        # address
        self.address = item["address"]
        # timeframe
        self.timeframe = period_timeframe()
        self.timeframe.from_dict(item["timeframe"])
        # status
        self.status = period_status()
        self.status.from_dict(item["status"])
        # fees
        self.fees = qtty_usd_yield()
        self.fees.from_dict(item["fees"])
        # fees_gamma
        self.fees_gamma = qtty_usd_yield()
        self.fees_gamma.from_dict(item["fees_gamma"])
        # rewards
        self.rewards = rewards_group()
        self.rewards.from_dict(item["rewards"])
        # fees_collected_within
        self.fees_collected_within = qtty_usd_yield()
        self.fees_collected_within.from_dict(item["fees_collected_within"])


# YIELD ANALISYS
class period_yield_analyzer:
    def __init__(
        self,
        chain: Chain,
        yield_data_list: list[period_yield_data],
        hypervisor_static: dict,
    ) -> None:
        # save base data
        self.chain = chain
        # filter yield_data_list outliers
        self.yield_data_list = self.discard_data_outliers(
            yield_data_list=yield_data_list
        )
        if not self.yield_data_list:
            raise Exception("No data to analyze")

        self.hypervisor_static = hypervisor_static

        # init other vars
        self._initialize()
        # execute analysis
        self._execute_analysis()

    def _initialize(self):
        # total period seconds
        self._total_seconds = 0

        # initial and end
        self._ini_price_per_share = self.yield_data_list[0].price_per_share_at_ini
        self._end_price_per_share = self.yield_data_list[-1].price_per_share
        self._ini_timestamp = self.yield_data_list[0].timeframe.ini.timestamp
        self._end_timestamp = self.yield_data_list[-1].timeframe.end.timestamp
        self._ini_prices = self.yield_data_list[0].status.ini.prices
        self._end_prices = self.yield_data_list[-1].status.end.prices
        self._ini_supply = self.yield_data_list[0].status.ini.supply
        self._end_supply = self.yield_data_list[-1].status.end.supply

        # price variation
        self._price_variation_token0 = Decimal("0")
        self._price_variation_token1 = Decimal("0")
        # deposit control var
        self._deposit_qtty_token0 = self.yield_data_list[
            0
        ].status.ini.underlying.qtty.token0
        self._deposit_qtty_token1 = self.yield_data_list[
            0
        ].status.ini.underlying.qtty.token1
        # fees ( period and aggregated )
        self._fees_qtty_token0_period = Decimal("0")
        self._fees_qtty_token1_period = Decimal("0")
        self._fees_usd_token0_period = Decimal("0")
        self._fees_usd_token1_period = Decimal("0")
        self._fees_usd_total_period = Decimal("0")
        self._fees_per_share_period = Decimal("0")
        self._fees_per_share_yield_period = Decimal("0")
        self._fees_qtty_token0_aggregated = Decimal("0")
        self._fees_qtty_token1_aggregated = Decimal("0")
        self._fees_usd_token0_aggregated = Decimal("0")
        self._fees_usd_token1_aggregated = Decimal("0")
        self._fees_usd_total_aggregated = Decimal("0")
        self._fees_per_share_aggregated = Decimal("0")
        self._fees_per_share_yield_aggregated = Decimal("0")
        # rewards ( period and aggregated )
        self._rewards_per_share_aggregated = Decimal("0")
        self._rewards_per_share_yield_aggregated = Decimal("0")
        self._rewards_usd_total_aggregated = Decimal("0")
        self._rewards_per_share_period = Decimal("0")
        self._rewards_per_share_yield_period = Decimal("0")
        self._rewards_usd_total_period = Decimal("0")
        self._rewards_token_symbols = set()
        # divergence ( period and aggregated )
        self._divergence_qtty_token0_aggregated = Decimal("0")
        self._divergence_qtty_token1_aggregated = Decimal("0")
        self._divergence_usd_token0_aggregated = Decimal("0")
        self._divergence_usd_token1_aggregated = Decimal("0")
        self._divergence_per_share_aggregated = Decimal("0")
        self._divergence_per_share_yield_aggregated = Decimal("0")
        self._divergence_usd_total_aggregated = Decimal("0")
        self._divergence_qtty_token0_period = Decimal("0")
        self._divergence_qtty_token1_period = Decimal("0")
        self._divergence_usd_token0_period = Decimal("0")
        self._divergence_usd_token1_period = Decimal("0")
        self._divergence_per_share_period = Decimal("0")
        self._divergence_per_share_yield_period = Decimal("0")
        self._divergence_usd_total_period = Decimal("0")

        # returns ( hypervisor and net returns ) -> hypervisor returns = fees + divergence and net returns = hypervisor returns + rewards
        self._hype_roi_usd_total_period = Decimal("0")
        self._hype_roi_qtty_token0_period = Decimal("0")
        self._hype_roi_qtty_token1_period = Decimal("0")
        self._hype_roi_per_share_period = Decimal("0")
        self._hype_roi_per_share_yield_period = Decimal("0")
        self._hype_roi_usd_total_aggregated = Decimal("0")
        self._hype_roi_qtty_token0_aggregated = Decimal("0")
        self._hype_roi_qtty_token1_aggregated = Decimal("0")
        self._hype_roi_per_share_aggregated = Decimal("0")
        self._hype_roi_per_share_yield_aggregated = Decimal("0")
        #
        self._net_roi_usd_total_period = Decimal("0")
        self._net_roi_per_share_period = Decimal("0")
        self._net_roi_per_share_yield_period = Decimal("0")
        self._net_roi_usd_total_aggregated = Decimal("0")
        self._net_roi_per_share_aggregated = Decimal("0")
        self._net_roi_per_share_yield_aggregated = Decimal("0")

        # comparison
        self._current_period_hodl_deposited = Decimal("0")
        self._period_hodl_deposited_yield = Decimal("0")
        self._period_hodl_fifty = Decimal("0")
        self._period_hodl_fifty_yield = Decimal("0")
        self._period_hodl_token0 = Decimal("0")
        self._period_hodl_token0_yield = Decimal("0")
        self._period_hodl_token1 = Decimal("0")
        self._period_hodl_token1_yield = Decimal("0")

        # graphic
        self._graph_data = []

    def _create_year_vars(self):
        # convert total seconds to decimal
        total_seconds = Decimal(str(self._total_seconds))
        day_in_seconds = 60 * 60 * 24
        year_in_seconds = Decimal(str(day_in_seconds * 365))
        # create vars
        self._year_fees_per_share_yield = (
            self._fees_per_share_yield_aggregated / total_seconds
            if total_seconds
            else Decimal("0")
        ) * year_in_seconds

        self._year_fees_qtty_usd = (
            self._fees_usd_total_aggregated / total_seconds
            if total_seconds
            else Decimal("0")
        ) * year_in_seconds
        self._year_fees_per_share = (
            self._fees_per_share_aggregated / total_seconds
            if total_seconds
            else Decimal("0")
        ) * year_in_seconds
        self._year_fees_qtty_token0 = (
            self._fees_qtty_token0_aggregated / total_seconds
            if total_seconds
            else Decimal("0")
        ) * year_in_seconds
        self._year_fees_qtty_token1 = (
            self._fees_qtty_token1_aggregated / total_seconds
            if total_seconds
            else Decimal("0")
        ) * year_in_seconds

        # period rewards to yearly extrapolation
        self._year_rewards_qtty_usd = (
            self._rewards_usd_total_aggregated / total_seconds
            if total_seconds
            else Decimal("0")
        ) * year_in_seconds
        self._year_rewards_per_share = (
            self._rewards_per_share_aggregated / total_seconds
            if total_seconds
            else Decimal("0")
        ) * year_in_seconds
        self._year_rewards_per_share_yield = (
            self._year_rewards_per_share / self._ini_price_per_share
            if self._ini_price_per_share
            else Decimal("0")
        )

        # period divergence to yearly extrapolation
        self._year_divergence_qtty_usd = (
            self._divergence_usd_total_aggregated / total_seconds
            if total_seconds
            else Decimal("0")
        ) * year_in_seconds
        self._year_divergence_per_share = (
            self._divergence_per_share_aggregated / total_seconds
            if total_seconds
            else Decimal("0")
        ) * year_in_seconds
        self._year_divergence_per_share_yield = (
            self._year_divergence_per_share / self._ini_price_per_share
            if self._ini_price_per_share
            else Decimal("0")
        )

        # period net yield to yearly extrapolation
        self._year_net_yield_qtty_usd = (
            self._net_roi_usd_total_aggregated / self._total_seconds
            if self._total_seconds
            else Decimal("0")
        ) * year_in_seconds
        self._year_net_yield_per_share = (
            self._net_roi_per_share_yield_aggregated / self._total_seconds
            if self._total_seconds
            else Decimal("0")
        ) * year_in_seconds
        self._year_net_yield_per_share_yield = (
            self._year_net_yield_per_share / self._ini_price_per_share
            if self._ini_price_per_share
            else Decimal("0")
        )

    def discard_data_outliers(
        self,
        yield_data_list: list[period_yield_data],
        max_items: int | None = None,
        max_reward_yield: float = 2.0,
        max_fees_yield: float = 2.0,
    ):
        """yield_data_list often contain initial data with humongous yields ( due to the init of the hype, Gamma team may be testing rewards, or injecting liquidity directly without using deposit [to mod token weights])"""
        result_list = []
        for idx, itm in enumerate(
            yield_data_list[:max_items] if max_items else yield_data_list
        ):
            if itm.period_seconds == 0:
                continue

            # divergence in absolute terms
            if abs(itm.divergence_per_share_percentage_yield) > max_reward_yield:
                continue

            # rewards vs tvl ratio
            _reward_yield = (
                (itm.rewards.usd or 0) / itm.ini_underlying_usd
                if itm.ini_underlying_usd
                else 0
            )
            if _reward_yield > max_reward_yield:
                continue

            if itm.fees_per_share_percentage_yield > max_fees_yield:
                continue

            # add to result
            result_list.append(itm)

        return result_list

    # COMPARISON PROPERTIES
    @property
    def deposit_qtty_usd(self):
        return (
            self._deposit_qtty_token0 * self._ini_prices.token0
            + self._deposit_qtty_token1 * self._ini_prices.token1
        )

    @property
    def fifty_qtty_token0(self):
        return (self.deposit_qtty_usd / 2) / self._ini_prices.token0

    @property
    def fifty_qtty_token1(self):
        return (self.deposit_qtty_usd / 2) / self._ini_prices.token1

    @property
    def hold_token0_qtty(self):
        return self.deposit_qtty_usd / self._ini_prices.token0

    @property
    def hold_token1_qtty(self):
        return self.deposit_qtty_usd / self._ini_prices.token1

    @property
    def period_hodl_deposited(self):
        """Whole period hodl deposited

        Returns:
            _type_: _description_
        """
        return (
            self._deposit_qtty_token0 * self._end_prices.token0
            + self._deposit_qtty_token1 * self._end_prices.token1
        )

    # MAIN METHODS
    def _execute_analysis(self):
        # fill variables
        self._find_initial_values()
        self._fill_variables()

    # LOOP
    def _fill_variables(self):
        for yield_item in self.yield_data_list:
            # add to total seconds
            self._total_seconds += yield_item.period_seconds

            # FEES ( does not need any previous data )
            self._fill_variables_fees(yield_item)

            # REWARDS ( does not need any previous data )
            self._fill_variables_rewards(yield_item)

            # RETURN HYPERVISOR ( does not need any previous data )
            self._fill_variables_hypervisor_return(yield_item)

            # divergence ( needs return to be processed first)
            self._fill_variables_divergence(yield_item)

            # RETURN NET ( needs return+fees+rewards+divergence to be processed first)
            self._fill_variables_net_return(yield_item)

            # PRICE VARIATION ( does not need any previous data )
            self._fill_variables_price(yield_item)

            # COMPARISON
            self._fill_variables_comparison(yield_item)

            # YEAR variables
            self._create_year_vars()

            # GRAPH ( needs all previous data )
            self._fill_graph(yield_item)

    # FILL VARIABLES
    def _fill_variables_fees(self, yield_item: period_yield_data):
        # FEES

        # fees for this timewindow ( line )
        self._fees_qtty_token0_period = yield_item.fees.qtty.token0 or Decimal("0")
        self._fees_qtty_token1_period = yield_item.fees.qtty.token1 or Decimal("0")
        self._fees_usd_token0_period = (
            yield_item.fees.qtty.token0 * yield_item.status.end.prices.token0
        )
        self._fees_usd_token1_period = (
            yield_item.fees.qtty.token1 * yield_item.status.end.prices.token1
        )
        self._fees_usd_total_period = yield_item.period_fees_usd or Decimal("0")
        # aggregated fees
        self._fees_qtty_token0_aggregated += self._fees_qtty_token0_period
        self._fees_qtty_token1_aggregated += self._fees_qtty_token1_period
        self._fees_usd_token0_aggregated += self._fees_usd_token0_period
        self._fees_usd_token1_aggregated += self._fees_usd_token1_period
        self._fees_usd_total_aggregated += self._fees_usd_total_period
        # fees per share
        self._fees_per_share_period = yield_item.fees_per_share or Decimal("0")
        self._fees_per_share_aggregated += yield_item.fees_per_share or Decimal("0")
        # yield
        self._fees_per_share_yield_period = (
            self._fees_per_share_period / yield_item.price_per_share_at_ini
            if yield_item.price_per_share_at_ini
            else Decimal("0")
        )
        self._fees_per_share_yield_aggregated = (
            self._fees_per_share_aggregated / self._ini_price_per_share
            if self._ini_price_per_share
            else Decimal("0")
        )

    def _fill_variables_rewards(self, yield_item: period_yield_data):
        # rewards for the period
        self._rewards_per_share_period = yield_item.rewards_per_share or Decimal("0")
        self._rewards_usd_total_period = yield_item.rewards.usd or Decimal("0")
        self._rewards_per_share_yield_period = (
            self._rewards_per_share_period / yield_item.price_per_share_at_ini
            if yield_item.price_per_share_at_ini
            else Decimal("0")
        )
        #
        self._rewards_per_share_aggregated += yield_item.rewards_per_share or Decimal(
            "0"
        )
        self._rewards_usd_total_aggregated += yield_item.rewards.usd or Decimal("0")
        self._rewards_per_share_yield_aggregated = (
            self._rewards_per_share_aggregated / self._ini_price_per_share
            if self._ini_price_per_share
            else Decimal("0")
        )

    def _fill_variables_divergence(self, yield_item: period_yield_data):
        # divergence (  share price end - share price ini - fees x share from the period[ so fees usd for the period/supply  ])
        self._divergence_qtty_token0_period = (
            yield_item.status.end.underlying.qtty.token0
            - yield_item.status.ini.underlying.qtty.token0
        ) - yield_item.fees.qtty.token0

        self._divergence_qtty_token1_period = (
            yield_item.status.end.underlying.qtty.token1
            - yield_item.status.ini.underlying.qtty.token1
        ) - yield_item.fees.qtty.token1

        self._divergence_usd_token0_period = (
            (
                yield_item.status.end.underlying.qtty.token0
                * yield_item.status.end.prices.token0
            )
            - (
                yield_item.status.ini.underlying.qtty.token0
                * yield_item.status.ini.prices.token0
            )
            - (yield_item.fees.qtty.token0 * yield_item.status.end.prices.token0)
        )

        self._divergence_usd_token1_period = (
            (
                yield_item.status.end.underlying.qtty.token1
                * yield_item.status.end.prices.token1
            )
            - (
                yield_item.status.ini.underlying.qtty.token1
                * yield_item.status.ini.prices.token1
            )
            - (yield_item.fees.qtty.token1 * yield_item.status.end.prices.token1)
        )

        self._divergence_usd_total_period = (
            self._divergence_usd_token0_period + self._divergence_usd_token1_period
        )

        self._divergence_per_share_period = (
            yield_item.price_per_share
            - yield_item.price_per_share_at_ini
            - (yield_item.period_fees_usd / yield_item.status.end.supply)
        )

        self._divergence_per_share_yield_period = (
            self._divergence_per_share_period / yield_item.price_per_share_at_ini
            if yield_item.price_per_share_at_ini
            else Decimal("0")
        )

        # aggregated
        self._divergence_qtty_token0_aggregated += self._divergence_qtty_token0_period
        self._divergence_qtty_token1_aggregated += self._divergence_qtty_token1_period
        self._divergence_usd_token0_aggregated += self._divergence_usd_token0_period
        self._divergence_usd_token1_aggregated += self._divergence_usd_token1_period
        self._divergence_usd_total_aggregated += self._divergence_usd_total_period
        # we do not use the *_period vars above to avoid errors like different end - ini price between periods or missing periods ( not common but possible )
        self._divergence_per_share_aggregated = (
            yield_item.price_per_share
            - self._ini_price_per_share
            - self._fees_per_share_aggregated
        )
        self._divergence_per_share_yield_aggregated = (
            self._divergence_per_share_aggregated / self._ini_price_per_share
            if self._ini_price_per_share
            else Decimal("0")
        )

    def _fill_variables_hypervisor_return(self, yield_item: period_yield_data):
        """Fees + divergence  ( no rewards included )"""

        self._hype_roi_usd_total_period = (
            yield_item.price_per_share * yield_item.status.end.supply
        ) - (yield_item.price_per_share_at_ini * yield_item.status.ini.supply)
        self._hype_roi_qtty_token0_period = (
            yield_item.status.end.underlying.qtty.token0
            - yield_item.status.ini.underlying.qtty.token0
        )
        self._hype_roi_qtty_token1_period = (
            yield_item.status.end.underlying.qtty.token1
            - yield_item.status.ini.underlying.qtty.token1
        )
        self._hype_roi_per_share_period = (
            yield_item.price_per_share - yield_item.price_per_share_at_ini
        )
        self._hype_roi_per_share_yield_period = (
            self._hype_roi_per_share_period / yield_item.price_per_share_at_ini
            if yield_item.price_per_share_at_ini
            else Decimal("0")
        )
        # aggregated
        self._hype_roi_usd_total_aggregated += self._hype_roi_usd_total_period
        self._hype_roi_qtty_token0_aggregated += self._hype_roi_qtty_token0_period
        self._hype_roi_qtty_token1_aggregated += self._hype_roi_qtty_token1_period
        self._hype_roi_per_share_aggregated += self._hype_roi_per_share_period
        self._hype_roi_per_share_yield_aggregated = (
            self._hype_roi_per_share_aggregated / self._ini_price_per_share
            if self._ini_price_per_share
            else Decimal("0")
        )

    def _fill_variables_net_return(self, yield_item: period_yield_data):
        """Roi + rewards ( so fees + divergence + rewards)"""

        self._net_roi_usd_total_period = (
            self._hype_roi_usd_total_period + self._rewards_usd_total_period
        )
        self._net_roi_per_share_period = (
            self._hype_roi_per_share_period + self._rewards_per_share_period
        )
        self._net_roi_per_share_yield_period = (
            self._net_roi_per_share_period / yield_item.price_per_share_at_ini
            if yield_item.price_per_share_at_ini
            else Decimal("0")
        )

        self._net_roi_usd_total_aggregated += self._net_roi_usd_total_period
        self._net_roi_per_share_aggregated += self._net_roi_per_share_period

        # this should be current share price + fees per share - initial share price / initial share price
        # self._net_roi_per_share_yield_aggregated = (
        #     self._net_roi_per_share_aggregated / self._ini_price_per_share
        #     if self._ini_price_per_share
        #     else Decimal("0")
        # )
        self._net_roi_per_share_yield_aggregated = (
            (
                (
                    (yield_item.price_per_share + self._rewards_per_share_aggregated)
                    - self._ini_price_per_share
                )
                / self._ini_price_per_share
            )
            if self._ini_price_per_share
            else Decimal("0")
        )

    def _fill_variables_price(self, yield_item: period_yield_data):
        self._price_variation_token0 = (
            (
                (yield_item.status.end.prices.token0 - self._ini_prices.token0)
                / self._ini_prices.token0
            )
            if self._ini_prices.token0
            else Decimal("0")
        )
        self._price_variation_token1 = (
            (
                (yield_item.status.end.prices.token1 - self._ini_prices.token1)
                / self._ini_prices.token1
            )
            if self._ini_prices.token1
            else Decimal("0")
        )

    def _fill_variables_comparison(self, yield_item: period_yield_data):
        # calculate self.current_period_hodl_deposited  as deposit qtty * yield_item end prices
        self._current_period_hodl_deposited = (
            self._deposit_qtty_token0 * yield_item.status.end.prices.token0
            + self._deposit_qtty_token1 * yield_item.status.end.prices.token1
        )
        self._period_hodl_deposited_yield = (
            (
                (self._current_period_hodl_deposited - self.deposit_qtty_usd)
                / self.deposit_qtty_usd
            )
            if self.deposit_qtty_usd
            else Decimal("0")
        )
        self._period_hodl_fifty = (
            self.fifty_qtty_token0 * yield_item.status.end.prices.token0
            + self.fifty_qtty_token1 * yield_item.status.end.prices.token1
        )
        self._period_hodl_fifty_yield = (
            ((self._period_hodl_fifty - self.deposit_qtty_usd) / self.deposit_qtty_usd)
            if self.deposit_qtty_usd
            else Decimal("0")
        )
        self._period_hodl_token0 = (
            self.hold_token0_qtty * yield_item.status.end.prices.token0
        )
        self._period_hodl_token0_yield = (
            ((self._period_hodl_token0 - self.deposit_qtty_usd) / self.deposit_qtty_usd)
            if self.deposit_qtty_usd
            else Decimal("0")
        )
        self._period_hodl_token1 = (
            self.hold_token1_qtty * yield_item.status.end.prices.token1
        )
        self._period_hodl_token1_yield = (
            ((self._period_hodl_token1 - self.deposit_qtty_usd) / self.deposit_qtty_usd)
            if self.deposit_qtty_usd
            else Decimal("0")
        )

    def _fill_graph(self, yield_item: period_yield_data):
        # build rewards details
        _rwds_details = {
            x: {"qtty": 0, "usd": 0, "seconds": 0, "period yield": 0}
            for x in self._rewards_token_symbols
        }

        if yield_item.rewards.details:
            for reward_detail in yield_item.rewards.details:
                _rwds_details[reward_detail["symbol"]] = {
                    "qtty": reward_detail["qtty"],
                    "usd": reward_detail["usd"],
                    "seconds": reward_detail["seconds"],
                    "period yield": reward_detail["period yield"],
                }
        # calculate per share prices ( to help when debugging)
        _status_ini = yield_item.status.ini.to_dict()
        _status_ini["prices"]["share"] = (
            (
                yield_item.status.ini.prices.token0
                * yield_item.status.ini.underlying.qtty.token0
                + yield_item.status.ini.prices.token1
                * yield_item.status.ini.underlying.qtty.token1
            )
            / yield_item.status.ini.supply
            if yield_item.status.ini.supply
            else 0
        )
        _status_end = yield_item.status.end.to_dict()
        _status_end["prices"]["share"] = (
            (
                yield_item.status.end.prices.token0
                * yield_item.status.end.underlying.qtty.token0
                + yield_item.status.end.prices.token1
                * yield_item.status.end.underlying.qtty.token1
            )
            / yield_item.status.end.supply
            if yield_item.status.end.supply
            else 0
        )
        # remove details from underlying data ( leave it empty )
        _status_ini["underlying"]["details"] = {}
        _status_end["underlying"]["details"] = {}

        # add to graph data
        self._graph_data.append(
            {
                "chain": self.chain.database_name,
                "address": self.hypervisor_static["address"],
                "symbol": self.hypervisor_static["symbol"],
                "block": yield_item.timeframe.end.block,
                "timestamp": yield_item.timeframe.end.timestamp,
                "timestamp_from": yield_item.timeframe.ini.timestamp,
                "datetime_from": f"{yield_item.timeframe.ini.datetime:%Y-%m-%d %H:%M:%S}",
                "datetime_to": f"{yield_item.timeframe.end.datetime:%Y-%m-%d %H:%M:%S}",
                "period_seconds": self._total_seconds,
                "status": {
                    "ini": _status_ini,
                    "end": _status_end,
                },
                "fees": {
                    "point": {
                        "yield": self._fees_per_share_yield_period,
                        "total_usd": self._fees_usd_total_period,
                        "qtty_token0": self._fees_qtty_token0_period,
                        "qtty_token1": self._fees_qtty_token1_period,
                        "usd_token0": self._fees_usd_token0_period,
                        "usd_token1": self._fees_usd_token1_period,
                        "per_share": self._fees_per_share_period,
                    },
                    "period": {
                        "yield": self._fees_per_share_yield_aggregated,
                        "total_usd": self._fees_usd_total_aggregated,
                        "qtty_token0": self._fees_qtty_token0_aggregated,
                        "qtty_token1": self._fees_qtty_token1_aggregated,
                        "usd_token0": self._fees_usd_token0_aggregated,
                        "usd_token1": self._fees_usd_token1_aggregated,
                        "per_share": self._fees_per_share_aggregated,
                    },
                    "year": {
                        "yield": self._year_fees_per_share_yield,
                        "total_usd": self._year_fees_qtty_usd,
                        "qtty_token0": self._year_fees_qtty_token0,
                        "qtty_token1": self._year_fees_qtty_token1,
                    },
                },
                "rewards": {
                    "point": {
                        "yield": self._rewards_per_share_yield_period,
                        "total_usd": self._rewards_usd_total_period,
                        "per_share": self._rewards_per_share_period,
                    },
                    "period": {
                        "yield": self._rewards_per_share_yield_aggregated,
                        "total_usd": self._rewards_usd_total_aggregated,
                        "per_share": self._rewards_per_share_aggregated,
                    },
                    "year": {
                        "yield": self._year_rewards_per_share_yield,
                        "total_usd": self._year_rewards_qtty_usd,
                    },
                    "details": _rwds_details,
                },
                "divergence": {
                    "point": {
                        "yield": self._divergence_per_share_yield_period,
                        "total_usd": self._divergence_usd_total_period,
                        "qtty_token0": self._divergence_qtty_token0_period,
                        "qtty_token1": self._divergence_qtty_token1_period,
                        "usd_token0": self._divergence_usd_token0_period,
                        "usd_token1": self._divergence_usd_token1_period,
                        "per_share": self._divergence_per_share_period,
                    },
                    "period": {
                        "yield": self._divergence_per_share_yield_aggregated,
                        "total_usd": self._divergence_usd_total_aggregated,
                        "qtty_token0": self._divergence_qtty_token0_aggregated,
                        "qtty_token1": self._divergence_qtty_token1_aggregated,
                        "usd_token0": self._divergence_usd_token0_aggregated,
                        "usd_token1": self._divergence_usd_token1_aggregated,
                        "per_share": self._divergence_per_share_aggregated,
                    },
                },
                "roi": {
                    "point": {
                        "return": self._net_roi_per_share_yield_period,
                        "total_usd": self._net_roi_usd_total_period,
                    },
                    "period": {
                        "return": self._net_roi_per_share_yield_aggregated,
                        "total_usd": self._net_roi_usd_total_aggregated,
                    },
                    "point_hypervisor": {
                        "return": self._hype_roi_per_share_yield_period,
                        "total_usd": self._hype_roi_usd_total_period,
                        "qtty_token0": self._hype_roi_qtty_token0_period,
                        "qtty_token1": self._hype_roi_qtty_token1_period,
                    },
                    "period_hypervisor": {
                        "return": self._hype_roi_per_share_yield_aggregated,
                        "total_usd": self._hype_roi_usd_total_aggregated,
                        "qtty_token0": self._hype_roi_qtty_token0_aggregated,
                        "qtty_token1": self._hype_roi_qtty_token1_aggregated,
                    },
                },
                "price": {
                    "period": {
                        "variation_token0": self._price_variation_token0,
                        "variation_token1": self._price_variation_token1,
                    },
                },
                "comparison": {
                    "return": {
                        "gamma": self._net_roi_per_share_yield_aggregated,
                        "hodl_deposited": self._period_hodl_deposited_yield,
                        "hodl_fifty": self._period_hodl_fifty_yield,
                        "hodl_token0": self._period_hodl_token0_yield,
                        "hodl_token1": self._period_hodl_token1_yield,
                    },
                    "gamma_vs": {
                        "hodl_deposited": (
                            (
                                (self._net_roi_per_share_yield_aggregated + 1)
                                / (self._period_hodl_deposited_yield + 1)
                            )
                            if self._period_hodl_deposited_yield != -1
                            else 0
                        )
                        - 1,
                        "hodl_fifty": (
                            (
                                (self._net_roi_per_share_yield_aggregated + 1)
                                / (self._period_hodl_fifty_yield + 1)
                            )
                            if self._period_hodl_fifty_yield != -1
                            else 0
                        )
                        - 1,
                        "hodl_token0": (
                            (
                                (self._net_roi_per_share_yield_aggregated + 1)
                                / (self._period_hodl_token0_yield + 1)
                            )
                            if self._period_hodl_token0_yield != -1
                            else 0
                        )
                        - 1,
                        "hodl_token1": (
                            (
                                (self._net_roi_per_share_yield_aggregated + 1)
                                / (self._period_hodl_token1_yield + 1)
                            )
                            if self._period_hodl_token1_yield != -1
                            else 0
                        )
                        - 1,
                    },
                },
            }
        )

    # GETTERS
    def get_graph(
        self, level: str | None = None, points_every: int | None = None
    ) -> list[dict]:
        if not level or level.lower() == "full":
            return self._graph_data
        elif level.lower() == "simple":
            # return only the fields needed for the simple graph

            ### NO FILTER ###
            if not points_every:
                return [
                    {
                        "chain": x["chain"],
                        "address": x["address"],
                        "symbol": x["symbol"],
                        "block": x["block"],
                        "timestamp": x["timestamp"],
                        #
                        "period_hodl_fifty": x["comparison"]["return"]["hodl_fifty"],
                        "period_hodl_token0": x["comparison"]["return"]["hodl_token0"],
                        "period_hodl_token1": x["comparison"]["return"]["hodl_token1"],
                        #
                        "period_gamma_netApr": x["roi"]["period"]["return"],
                        "period_gamma_feeApr": x["fees"]["period"]["yield"],
                        "period_gamma_rewardsApr": x["rewards"]["period"]["yield"],
                        "period_gamma_divergenceApr": x["divergence"]["period"][
                            "yield"
                        ],
                        #
                        "shares": {
                            "price_per_share": x["status"]["end"]["prices"]["share"],
                            "period_fees_per_share": x["fees"]["period"]["per_share"],
                            "period_rewards_per_share": x["rewards"]["period"][
                                "per_share"
                            ],
                            "period_divergence_per_share": x["divergence"]["period"][
                                "per_share"
                            ],
                            "total_shares": x["status"]["end"]["supply"],
                        },
                        "token0_price": x["status"]["end"]["prices"]["token0"],
                        "token1_price": x["status"]["end"]["prices"]["token1"],
                        "data": {
                            # USD total
                            "period_fees_usd_total": x["fees"]["period"]["total_usd"],
                            "period_rewards_usd_total": x["rewards"]["period"][
                                "total_usd"
                            ],
                            # QTTY
                            "period_fees_qtty_token0": x["fees"]["period"][
                                "qtty_token0"
                            ],
                            "period_fees_usd_token0": x["fees"]["period"]["usd_token0"],
                            "period_fees_qtty_token1": x["fees"]["period"][
                                "qtty_token1"
                            ],
                            "period_fees_usd_token1": x["fees"]["period"]["usd_token1"],
                        },
                    }
                    for x in self._graph_data
                ]

            ### FILTER ###
            _last_timestamp = 0

            def _should_include(item: dict) -> bool:
                nonlocal _last_timestamp

                if _last_timestamp == 0:
                    _last_timestamp = item["timestamp"]
                    return True

                # add control point
                if item["timestamp"] - _last_timestamp >= points_every:
                    _last_timestamp = item["timestamp"]
                    return True
                else:
                    return False

            # control points var
            filtered_data = list(filter(_should_include, self._graph_data))
            # Ensure the last item is included if not already in filtered_data ( last item is the most recent )
            if self._graph_data and (self._graph_data[-1] not in filtered_data):
                filtered_data.append(self._graph_data[-1])
                
            result = [
                {
                    "chain": x["chain"],
                    "address": x["address"],
                    "symbol": x["symbol"],
                    "block": x["block"],
                    "timestamp": x["timestamp"],
                    #
                    "period_hodl_fifty": x["comparison"]["return"]["hodl_fifty"],
                    "period_hodl_token0": x["comparison"]["return"]["hodl_token0"],
                    "period_hodl_token1": x["comparison"]["return"]["hodl_token1"],
                    #
                    "period_gamma_netApr": x["roi"]["period"]["return"],
                    "period_gamma_feeApr": x["fees"]["period"]["yield"],
                    "period_gamma_rewardsApr": x["rewards"]["period"]["yield"],
                    "period_gamma_divergenceApr": x["divergence"]["period"]["yield"],
                    #
                    "shares": {
                        "price_per_share": x["status"]["end"]["prices"]["share"],
                        "period_fees_per_share": x["fees"]["period"]["per_share"],
                        "period_rewards_per_share": x["rewards"]["period"]["per_share"],
                        "period_divergence_per_share": x["divergence"]["period"][
                            "per_share"
                        ],
                        "total_shares": x["status"]["end"]["supply"],
                    },
                    "token0_price": x["status"]["end"]["prices"]["token0"],
                    "token1_price": x["status"]["end"]["prices"]["token1"],
                    "data": {
                        # USD total
                        "period_fees_usd_total": x["fees"]["period"]["total_usd"],
                        "period_rewards_usd_total": x["rewards"]["period"]["total_usd"],
                        # QTTY
                        "period_fees_qtty_token0": x["fees"]["period"]["qtty_token0"],
                        "period_fees_usd_token0": x["fees"]["period"]["usd_token0"],
                        "period_fees_qtty_token1": x["fees"]["period"]["qtty_token1"],
                        "period_fees_usd_token1": x["fees"]["period"]["usd_token1"],
                    },
                }
                for x in filtered_data
            ]

            # insert first item if there is only one item
            if len(result) == 1:
                result.insert(
                    0,
                    {
                        "chain": filtered_data[0]["chain"],
                        "address": filtered_data[0]["address"],
                        "symbol": filtered_data[0]["symbol"],
                        "block": filtered_data[0]["block"],
                        "timestamp": filtered_data[0]["timestamp_from"],
                        #
                        "period_hodl_fifty": 0,
                        "period_hodl_token0": 0,
                        "period_hodl_token1": 0,
                        #
                        "period_gamma_netApr": 0,
                        "period_gamma_feeApr": 0,
                        "period_gamma_rewardsApr": 0,
                        "period_gamma_divergenceApr": 0,
                        #
                        "shares": {
                            "price_per_share": filtered_data[0]["status"]["ini"][
                                "prices"
                            ]["share"],
                            "period_fees_per_share": 0,
                            "period_rewards_per_share": 0,
                            "period_divergence_per_share": 0,
                            "total_shares": filtered_data[0]["status"]["end"]["supply"],
                        },
                        "token0_price": filtered_data[0]["status"]["ini"]["prices"][
                            "token0"
                        ],
                        "token1_price": filtered_data[0]["status"]["ini"]["prices"][
                            "token1"
                        ],
                        "data": {
                            # USD total
                            "period_fees_usd_total": 0,
                            "period_rewards_usd_total": 0,
                            # QTTY
                            "period_fees_qtty_token0": 0,
                            "period_fees_usd_token0": 0,
                            "period_fees_qtty_token1": 0,
                            "period_fees_usd_token1": 0,
                        },
                    },
                )

            return result

    def get_graph_csv(self):
        """Return a csv string with the graph data

        Returns:
            str: csv string
        """
        # return csv string
        return convert_to_csv(self._graph_data)

    def get_rewards_detail(self):
        result = {}

        for item in self.yield_data_list:
            if item.rewards.details:
                for detail in item.rewards.details:
                    # add to result if not exists already
                    if not detail["symbol"] in result:
                        result[detail["symbol"]] = {
                            "qtty": 0,
                            "usd": 0,
                            "seconds": 0,
                            "period yield": 0,
                        }
                    # add to result
                    result[detail["symbol"]]["qtty"] += detail["qtty"]
                    result[detail["symbol"]]["usd"] += detail["usd"]
                    result[detail["symbol"]]["seconds"] += detail["seconds"]
                    result[detail["symbol"]]["period yield"] += detail["period yield"]

        return result

    # HELPERS
    def _find_initial_values(self):
        for yield_item in self.yield_data_list:
            # TODO: REMOVE FROM HERE bc its already done in the loop
            # add total seconds
            # self._total_seconds += yield_item.period_seconds

            # define the different reward tokens symbols ( if any ) to be used in graphic exports
            if yield_item.rewards.details:
                for reward_detail in yield_item.rewards.details:
                    self._rewards_token_symbols.add(reward_detail["symbol"])

            # modify initial and end values, if needed ( should not be needed bc its sorted by timestamp)
            if yield_item.timeframe.ini.timestamp < self._ini_timestamp:
                self._ini_timestamp = yield_item.timeframe.ini.timestamp
                self._ini_price_per_share = yield_item.price_per_share_at_ini
                self._ini_prices = yield_item.status.ini.prices
                self._ini_supply = yield_item.status.ini.supply
                self._deposit_qtty_token0 = yield_item.status.ini.underlying.qtty.token0
                self._deposit_qtty_token1 = yield_item.status.ini.underlying.qtty.token1

            if yield_item.timeframe.end.timestamp > self._end_timestamp:
                self._end_timestamp = yield_item.timeframe.end.timestamp
                self._end_price_per_share = yield_item.price_per_share
                self._end_prices = yield_item.status.end.prices
                self._end_supply = yield_item.status.end.supply

    def get_token_usd_weight(
        self, yield_item: period_yield_data
    ) -> tuple[Decimal, Decimal]:
        """Extract current underlying token weights vs current usd price

        Args:
            yield_item (period_yield_data):
        """
        _token0_end_usd_value = (
            yield_item.status.end.prices.token0
            * yield_item.status.end.underlying.qtty.token0
        )
        _token1_end_usd_value = (
            yield_item.status.end.prices.token1
            * yield_item.status.end.underlying.qtty.token1
        )
        _temp = _token0_end_usd_value + _token1_end_usd_value
        _token0_percentage = _token0_end_usd_value / _temp if _temp else Decimal("0")
        _token1_percentage = _token1_end_usd_value / _temp if _temp else Decimal("0")
        return _token0_percentage, _token1_percentage
