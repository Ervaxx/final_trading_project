import asyncio
import logging
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from .base import MarketProvider
from .types import DataType, MarketRequest, Timeframe
from .cache import MarketCache  # Ensure this import is correct

class YFinanceProvider(MarketProvider):
    def __init__(self, cache_expiry_minutes: int = 5):
        """
        Initialize the YFinanceProvider with a cache and logger.
        """
        self.cache = MarketCache[DataType](expiry_minutes=cache_expiry_minutes)
        self.logger = logging.getLogger(self.__class__.__name__)

    async def get_historical_data(self, requests: List[MarketRequest]) -> Dict[str, pd.DataFrame]:
        """
        Get historical market data with the latest updates for multiple symbols.

        Args:
            requests (List[MarketRequest]): List of market requests for each symbol.

        Returns:
            Dict[str, pd.DataFrame]: A dictionary mapping symbols to their data.
        """
        tasks = [self._get_symbol_historical_data(request) for request in requests]
        results = await asyncio.gather(*tasks)
        return {request.symbol: data for request, data in zip(requests, results) if data is not None}

    async def _get_symbol_historical_data(self, request: MarketRequest) -> Optional[pd.DataFrame]:
        """
        Get historical market data with the latest updates for a single symbol.
        """
        symbol = request.symbol
        timeframe = request.timeframe
        try:
            cache_key = f"{symbol}_{timeframe.value}"
            cached_data = self.cache.get(cache_key)

            if cached_data is not None:
                self.logger.info(f"Cache hit for {symbol}")
                self.logger.info(f"Cached data range: {cached_data.index[0]} to {cached_data.index[-1]}")

                # Fetch new data from the last cached time
                last_cached_time = cached_data.index[-1]
                latest_data = await self._fetch_data_since(symbol, last_cached_time, timeframe)

                if latest_data is not None and not latest_data.empty:
                    # Combine with cached data
                    updated_data = pd.concat([cached_data, latest_data])
                    # Remove duplicates
                    updated_data = updated_data[~updated_data.index.duplicated(keep='last')]
                    # Maintain a rolling 5-day window
                    cutoff = datetime.utcnow() - timedelta(days=5)
                    updated_data = updated_data[updated_data.index >= cutoff]
                    self.cache.set(cache_key, updated_data)
                    return updated_data

                # No new data; return cached data
                return cached_data

            # Cache miss; fetch historical data
            self.logger.info(f"Cache miss for {symbol}, fetching 5-day history")
            data = await self._fetch_historical(symbol, timeframe)
            if data is not None:
                self.cache.set(cache_key, data)
                return data

            self.logger.warning(f"No data available for {symbol}")
            return None

        except Exception as e:
            self.logger.error(f"Error getting historical data for {symbol}: {str(e)}")
            return None

    async def get_latest_data(self, symbols: List[str]) -> Dict[str, Optional[pd.Series]]:
        """
        Get the latest market data points for multiple symbols.

        Args:
            symbols (List[str]): List of stock ticker symbols.

        Returns:
            Dict[str, Optional[pd.Series]]: A dictionary mapping symbols to their latest data point.
        """
        tasks = [self._get_symbol_latest_data(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        return {symbol: data for symbol, data in zip(symbols, results) if data is not None}

    async def _get_symbol_latest_data(self, symbol: str) -> Optional[pd.Series]:
        """
        Get the latest market data point for a single symbol.
        """
        try:
            self.logger.info(f"Fetching latest data for symbol: {symbol}")
            data = await self._fetch_latest(symbol)
            if data is not None and not data.empty:
                latest = data.iloc[-1]
                return latest
            else:
                self.logger.warning(f"No latest data available for {symbol}")
                return None
        except Exception as e:
            self.logger.error(f"Error fetching latest data for {symbol}: {str(e)}")
            return None

    async def _fetch_historical(self, symbol: str, timeframe: Timeframe) -> Optional[pd.DataFrame]:
        """
        Fetch 5 days of data for a single symbol.
        """
        try:
            interval = timeframe.value
            data = await asyncio.to_thread(
                yf.download,
                tickers=symbol,
                period='5d',
                interval=interval,
                progress=False
            )

            if data.empty:
                self.logger.warning(f"No historical data returned for {symbol}")
                return None

            # Drop the symbol level if present
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.droplevel(0)

            data.index = data.index.tz_localize(None)
            return data

        except Exception as e:
            self.logger.error(f"Error fetching historical data for {symbol}: {str(e)}")
            return None

    async def _fetch_latest(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Fetch the latest data for today for a single symbol.

        Args:
            symbol (str): Stock ticker symbol.

        Returns:
            Optional[pd.DataFrame]: DataFrame with the latest data.
        """
        try:
            data = await asyncio.to_thread(
                yf.download,
                tickers=symbol,
                period='1d',
                interval='1m',
                progress=False
            )

            if data.empty:
                self.logger.warning(f"No latest data returned for {symbol}")
                return None

            data.index = data.index.tz_localize(None)
            return data

        except Exception as e:
            self.logger.error(f"Error fetching latest data for {symbol}: {str(e)}")
            return None

    async def _fetch_data_since(self, symbol: str, last_time: pd.Timestamp, timeframe: Timeframe) -> Optional[pd.DataFrame]:
        """
        Fetch data from the last cached time to now for a single symbol.

        Args:
            symbol (str): Stock ticker symbol.
            last_time (pd.Timestamp): Last cached timestamp.
            timeframe (Timeframe): Timeframe for the data.

        Returns:
            Optional[pd.DataFrame]: DataFrame with the new data.
        """
        try:
            # Add a small delta to avoid overlapping data
            start_time = last_time + timedelta(minutes=1)
            end_time = datetime.utcnow()

            # Convert times to strings in the format required by yfinance
            start_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
            end_str = end_time.strftime('%Y-%m-%d %H:%M:%S')

            data = await asyncio.to_thread(
                yf.download,
                tickers=symbol,
                interval=timeframe.value,
                start=start_str,
                end=end_str,
                progress=False
            )

            if data.empty:
                self.logger.info(f"No new data available for {symbol} since {start_str}")
                return None

            data.index = data.index.tz_localize(None)
            return data

        except Exception as e:
            self.logger.error(f"Error fetching data since {last_time} for {symbol}: {str(e)}")
            return None
