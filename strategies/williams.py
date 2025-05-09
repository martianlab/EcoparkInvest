import pandas as pd
from strategies import BaseStrategy, register

@register('chaos_breakout')
class ChaosBreakoutStrategy(BaseStrategy):
    """
    Chaos Breakout (Bill Williams):
    – Alligator + Fractals + Awesome Oscillator + Accelerator Oscillator
    – вход в лонг: цена пробила последний верхний фрактал,
      Alligator проснулся (lips>teeth>jaw), AO>0 и AC>0
    – вход в шорт: цена пробила последний нижний фрактал,
      Alligator проснулся вниз (lips<teeth<jaw), AO<0 и AC<0
    – размер позиции = risk_target
    """
    def __init__(
        self,
        jaw: int = 13, teeth: int = 8, lips: int = 5,
        jaw_shift: int = 8, teeth_shift: int = 5, lips_shift: int = 3,
        ao_fast: int = 5, ao_slow: int = 34, ac_slow: int = 5,
        fractal_window: int = 5,
        risk_target: float = 0.01
    ):
        # Alligator params
        self.jaw = jaw; self.teeth = teeth; self.lips = lips
        self.jaw_shift = jaw_shift; self.teeth_shift = teeth_shift; self.lips_shift = lips_shift
        # AO / AC params
        self.ao_fast = ao_fast; self.ao_slow = ao_slow; self.ac_slow = ac_slow
        # Fractal window (must be odd, default 5)
        self.fractal_w = fractal_window
        # Position sizing
        self.risk = risk_target
        self.reasons = {}

    def _awesome(self, price: pd.Series) -> pd.Series:
        median = (price + price.shift(1) + price.shift(-1)) / 3  # approximate mid-price
        ao = median.rolling(self.ao_fast).mean() - median.rolling(self.ao_slow).mean()
        return ao

    def _ac(self, ao: pd.Series) -> pd.Series:
        return ao - ao.rolling(self.ac_slow).mean()

    def _fractals(self, high: pd.Series, low: pd.Series):
        w = self.fractal_w
        # detect central fractal: index offset = w//2
        shift = w // 2
        # create boolean masks
        is_high = (
            (high.shift( shift) < high) &
            (high.shift(shift-1) < high) &
            (high.shift(shift+1) < high) &
            (high.shift(-shift) < high)
        )
        is_low = (
            (low.shift( shift) > low) &
            (low.shift(shift-1) > low) &
            (low.shift(shift+1) > low) &
            (low.shift(-shift) > low)
        )
        return is_high, is_low

    def _single(self, df: pd.DataFrame) -> pd.Series:
        price = df['Close']
        high  = df['High']; low = df['Low']

        # 1) Alligator lines
        jaw_line   = price.ewm(span=self.jaw,   adjust=False).mean().shift(self.jaw_shift)
        teeth_line = price.ewm(span=self.teeth, adjust=False).mean().shift(self.teeth_shift)
        lips_line  = price.ewm(span=self.lips,  adjust=False).mean().shift(self.lips_shift)

        # 2) Fractals
        is_high, is_low = self._fractals(high, low)
        # Last fractal levels, shifted to avoid same-bar breakout
        last_high = high.where(is_high).ffill().shift(1)
        last_low  = low.where(is_low).ffill().shift(1)

        # 3) AO & AC
        ao = self._awesome(price)
        ac = self._ac(ao)

        # 4) Entry signals
        long_break  = price > last_high
        short_break = price < last_low

        all_long = (lips_line > teeth_line) & (teeth_line > jaw_line)
        all_short = (lips_line < teeth_line) & (teeth_line < jaw_line)

        ao_pos = ao > 0
        ac_pos = ac > 0
        ao_neg = ao < 0
        ac_neg = ac < 0

        long_signal  = long_break  & all_long  & ao_pos  & ac_pos
        short_signal = short_break & all_short & ao_neg & ac_neg

        pos = long_signal.astype(float) - short_signal.astype(float)
        return (pos * self.risk).fillna(0)

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        result = pd.DataFrame(index=df.index)
        syms = df.columns.get_level_values(0).unique()
        for sym in syms:
            intraday = df[sym]
            # ensure enough data for fractals
            if len(intraday) < self.fractal_w:
                self.reasons[sym] = f"недостаточно данных для фракталов (<{self.fractal_w})"
                result[sym] = 0
            else:
                result[sym] = self._single(intraday)
        return result.fillna(0)