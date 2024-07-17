from datetime import datetime

import pandas as pd

from sources.mongo.bins.helpers import perps_database_helper


async def backtests(token, timeframe, strategy, lookback, start, end):
    db_data = await perps_database_helper().get_backtests(
        token, timeframe, strategy, lookback, start, end
    )

    df_data = pd.DataFrame(db_data)
    df_data = df_data[["timestamp", "close", "strategy_return"]]
    df_data["hold_return"] = df_data.close / df_data.close.shift(1) - 1
    df_data.fillna(0, inplace=True)
    # df_data.timestamp = df_data.timestamp.dt.strftime('%Y-%m-%d %H:%M:%S')

    print(df_data)

    return df_data[[
        "timestamp",
        "hold_return",
        "strategy_return"
    ]].to_dict("records")
