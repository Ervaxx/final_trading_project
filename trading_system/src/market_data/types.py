# src/market_data/market_types.py
from dataclasses import dataclass
from enum import Enum
from typing import Union
import pandas as pd

DataType = Union[pd.DataFrame, pd.Series]

class Timeframe(Enum):
    MINUTE = '1m'
    FIVE_MINUTES = '5m'
    HOURLY = '1h'
    DAILY = '1d'

@dataclass
class MarketRequest:
    symbol: str
    timeframe: Timeframe
