from typing import List
import importlib, pkgutil, pandas as pd
from strategies import REGISTRY, parse_strat_name

class PortfolioManager:
    def __init__(self, strat_names: List[str]):
        # Импортируем все модули в strategies/, чтобы REGISTRY был полностью заполнен
        self._import_all_strats()
        self.strategies = []
        for name in strat_names:
            base, params = parse_strat_name(name)
            if base not in REGISTRY:
                raise ValueError(f"Unknown strategy: {base}")
            if base == "pair_reversion" and params:
                self.strategies.append(REGISTRY[base](pair=tuple(params)))
            else:
                self.strategies.append(REGISTRY[base]())

    @staticmethod
    def _import_all_strats():
        # Берём реальный путь пакета strategies
        import strategies
        for finder, module_name, is_pkg in pkgutil.iter_modules(strategies.__path__):
            # импортируем каждый файл strategies/<module_name>.py
            importlib.import_module(f"strategies.{module_name}")

    def generate_weights(self, df: pd.DataFrame) -> pd.DataFrame:
        weights = pd.DataFrame(index=df.index)
        for st in self.strategies:
            weights = weights.add(st.generate(df), fill_value=0)
        # риск-контроль: суммарное плечо ≤ 1
        leverage = weights.abs().sum(axis=1)
        return weights.div(leverage.where(leverage > 1, 1), axis=0)