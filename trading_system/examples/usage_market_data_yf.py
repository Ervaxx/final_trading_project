import sys
import os
import asyncio
import logging
from datetime import datetime
import pandas as pd

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.market_data.types import MarketRequest, Timeframe
from src.market_data.yfinance_provider import YFinanceProvider

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    provider = YFinanceProvider()

    symbols = ["AAPL", "TSLA", "GOOG"]  # Multiple symbols

    print("\n=== Fetching Latest Data for Multiple Symbols ===")
    latest_data_dict = await provider.get_latest_data(symbols)

    for symbol in symbols:
        latest = latest_data_dict.get(symbol)
        if latest is not None:
            print(f"\nFound data for {symbol}:")
            for column in ['Open', 'High', 'Low', 'Close', 'Volume']:
                value = latest[column]

                # If value is a Series, extract the scalar value
                if isinstance(value, pd.Series):
                    value = value.iloc[0]

                if column == 'Volume':
                    print(f"{column}: {value:,}")
                else:
                    print(f"{column}: ${value:.2f}")
            print(f"Time: {latest.name}")
        else:
            print(f"\nNo data found for {symbol}")


    print("\n=== Fetching Historical Data for Multiple Symbols ===")
    requests = [MarketRequest(symbol=symbol, timeframe=Timeframe.MINUTE) for symbol in symbols]
    historical_data_dict = await provider.get_historical_data(requests)

    for symbol in symbols:
        latest = latest_data_dict.get(symbol)
        if latest is not None:
            print(f"\nFound data for {symbol}:")
            for column in ['Open', 'High', 'Low', 'Close', 'Volume']:
                value = latest[column]

                # If value is a Series, extract the scalar value
                if isinstance(value, pd.Series):
                    value = value.iloc[0]

                if column == 'Volume':
                    print(f"{column}: {value:,}")
                else:
                    print(f"{column}: ${value:.2f}")
            print(f"Time: {latest.name}")
        else:
            print(f"\nNo data found for {symbol}")


    print("\n=== Fetching Updated Historical Data for Multiple Symbols ===")
    updated_historical_data_dict = await provider.get_historical_data(requests)

    for symbol in symbols:
        data = updated_historical_data_dict.get(symbol)
        if data is not None:
            print(f"\nUpdated historical data for {symbol}:")
            print(f"Total data points: {len(data)}")
            print(f"Data range: {data.index[0]} to {data.index[-1]}")
        else:
            print(f"\nNo updated historical data found for {symbol}")

if __name__ == "__main__":
    asyncio.run(main())