from datetime import datetime, timedelta
from typing import Dict, Optional, Generic, TypeVar
import logging

T = TypeVar('T')

class MarketCache(Generic[T]):
    def __init__(self, expiry_minutes: int = 5):
        self._cache: Dict[str, T] = {}
        self._timestamps: Dict[str, datetime] = {}
        self.expiry = timedelta(minutes=expiry_minutes)
        self.logger = logging.getLogger(self.__class__.__name__)

    def get(self, key: str) -> Optional[T]:
        if key not in self._cache:
            return None
        
        cached_time = self._timestamps.get(key)
        if cached_time and datetime.now() - cached_time > self.expiry:
            self.logger.info(f"Cache expired for {key}")
            self._cache.pop(key)
            self._timestamps.pop(key)
            return None
            
        return self._cache.get(key)

    def set(self, key: str, value: T) -> None:
        self._cache[key] = value
        self._timestamps[key] = datetime.now()
        self.logger.info(f"Cached data for {key}")

    def clear(self, key: Optional[str] = None) -> None:
        if key:
            self._cache.pop(key, None)
            self._timestamps.pop(key, None)
        else:
            self._cache.clear()
            self._timestamps.clear()