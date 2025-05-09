from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd
REGISTRY: Dict[str, 'BaseStrategy'] = {}

def register(name):
    def deco(cls):
        REGISTRY[name] = cls
        return cls
    return deco

class BaseStrategy(ABC):
    @abstractmethod
    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return DataFrame(date × symbol) weights"""

    @property
    def name(self):
        return self.__class__.__name__

def parse_strat_name(name):
    if ':' in name:
        base, args = name.split(':', 1)
        params = args.split(',')
        return base, params
    return name, []