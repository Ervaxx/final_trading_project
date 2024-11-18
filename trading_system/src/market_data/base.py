from abc import ABC, abstractmethod
from typing import Optional
from .types import DataType, MarketRequest

class MarketProvider(ABC):
    @abstractmethod
    async def get_historical_data(self, request: MarketRequest) -> Optional[DataType]:
        """Get historical market data"""
        pass

    @abstractmethod
    async def get_latest_data(self, symbol: str) -> Optional[DataType]:
        """Get latest market data"""
        pass