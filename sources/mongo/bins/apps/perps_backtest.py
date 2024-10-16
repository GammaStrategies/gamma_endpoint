from datetime import datetime

import pandas as pd

from sources.mongo.bins.helpers import perps_database_helper


async def backtests(token: str, timeframe: str, strategy: str, lookback: int, leverage: int, start, end):
    db_data = await perps_database_helper().get_backtests(
        token, timeframe, strategy, lookback, f"{leverage:.2f}", start, end
    )

    df_data = pd.DataFrame(db_data)
    df_data = df_data[["timestamp", "close", "strategy_return"]]
    df_data.loc[0, "strategy_return"] = 0
    df_data["hold_return"] = df_data.close / df_data.close.shift(1) - 1
    df_data.fillna(0, inplace=True)
    df_data["cumulative_hold_return"] = df_data.hold_return.add(1).cumprod()
    df_data["cumulative_strategy_return"] = df_data.strategy_return.add(1).cumprod()


    return df_data[[
        "timestamp",
        "hold_return",
        "strategy_return",
        "cumulative_hold_return",
        "cumulative_strategy_return"
    ]].to_dict("records")
