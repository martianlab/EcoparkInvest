from typing import List
import importlib, pandas as pd
from strategies import REGISTRY, parse_strat_name

class PortfolioManager:
    def __init__(self, strat_names: List[str]):
        # сначала подгружаем модули, чтобы REGISTRY заполнился
        self._import_strats(strat_names)
        # затем создаём инстансы со всеми параметрами
        self.strategies = []
        for name in strat_names:
            base, params = parse_strat_name(name)
            if base not in REGISTRY:
                raise ValueError(f"Unknown strategy: {base}")
            if base == "pair_reversion" and params:
                # специальный кейс для pair_reversion
                self.strategies.append(REGISTRY[base](pair=tuple(params)))
            else:
                self.strategies.append(REGISTRY[base]())

    @staticmethod
    def _import_strats(names):
        """
        Пытаемся импортировать strategies.<base>;
        если нет модуля, пробуем strategies.intraday (там лежат все intraday-стратегии).
        """
        for n in names:
            base = n.split(":", 1)[0]
            if base in REGISTRY:
                continue
            try:
                importlib.import_module(f"strategies.{base}")
            except ModuleNotFoundError:
                # fallback для intraday-стратегий
                try:
                    importlib.import_module("strategies.intraday")
                except ModuleNotFoundError:
                    # если и здесь не нашлось — ошибка
                    raise ImportError(f"Cannot import strategy module for '{base}'")
            # после импорта проверим, зарегистрировалась ли стратегия
            if base not in REGISTRY:
                raise ValueError(f"Strategy '{base}' not found in REGISTRY after import")

    def generate_weights(self, df: pd.DataFrame) -> pd.DataFrame:
        weights = pd.DataFrame(index=df.index)
        for st in self.strategies:
            weights = weights.add(st.generate(df), fill_value=0)
        # риск-контроль: суммарное плечо ≤ 1
        leverage = weights.abs().sum(axis=1)
        return weights.div(leverage.where(leverage > 1, 1), axis=0)