from datetime import datetime

import numpy as np
import pandas as pd

class PerpBacktest:
    """Base class for Perps backtest"""

    def __init__(self, strategy: str, token: str, timeframe: str, lookback: int):
        self.token = token.upper()
        self.strategy = strategy
        self.timeframe = timeframe
        self.lookback = lookback
        self.data: pd.DataFrame
        self.signals: pd.DataFrame | None = None
        self.results: pd.DataFrame

    def load_signals_csv(self):
        """Load signals data from CSV file"""
        filepath = f"signals_{self.token}_{self.timeframe}_{self.strategy}.csv"
        signals = pd.read_csv(filepath)
        signals.timestamp = pd.to_datetime(signals.timestamp)

        self.signals = signals


    def run(self, start: datetime, end: datetime, leverage: float, trade_cost: float):
        """Run backtest"""
        if not self.signals:
            self.load_signals_csv()

        df_run = self.signals

        df_run = df_run[
            (start <= df_run.timestamp) & (df_run.timestamp <= end)
        ].reset_index(drop=True)
        df_run["cash"] = 0.0
        df_run.loc[0, "cash"] = 100000.0
        df_run["quantity"] = 0.0
        df_run["entry"] = 0.0
        df_run["value"] = 0.0

        for i in range(len(df_run) - 1):

            signal = df_run.loc[i, "signal"]
            quantity = df_run.loc[i, "quantity"]

            # position updating
            if abs(np.sign(quantity) - signal) > 0:
                cash = (
                    df_run.loc[i, "value"] * (1 - trade_cost) + df_run.loc[i, "cash"]
                )  # closing cost
                df_run.loc[i + 1, "cash"] = cash
                if signal != 0:
                    price = df_run.loc[i, "close"]
                    df_run.loc[i + 1, "cash"] = 0
                    df_run.loc[i + 1, "quantity"] = (
                        signal * cash / price * leverage * (1 - trade_cost)
                    )  # opening cost
                    df_run.loc[i + 1, "entry"] = price
                    df_run.loc[i + 1, "value"] = abs(df_run.loc[i + 1, "quantity"]) * (
                        df_run.loc[i + 1, "entry"] / leverage
                        + np.sign(df_run.loc[i + 1, "quantity"])
                        * (df_run.loc[i + 1, "close"] - df_run.loc[i + 1, "entry"])
                    )  # locked collateral + pnl
            else:
                df_run.loc[i + 1, "cash"] = df_run.loc[i, "cash"]
                df_run.loc[i + 1, "quantity"] = quantity
                df_run.loc[i + 1, "entry"] = df_run.loc[i, "entry"]
                df_run.loc[i + 1, "value"] = abs(df_run.loc[i + 1, "quantity"]) * (
                    df_run.loc[i + 1, "entry"] / leverage
                    + np.sign(df_run.loc[i + 1, "quantity"])
                    * (df_run.loc[i + 1, "close"] - df_run.loc[i + 1, "entry"])
                )  # locked collateral + pnl

        df_run["strategy_return"] = (df_run.value + df_run.cash) / (
            df_run.value + df_run.cash
        ).shift(1) - 1

        print(df_run)

        self.results = df_run[["timestamp", "return", "strategy_return"]]
