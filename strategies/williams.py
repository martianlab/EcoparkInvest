import pandas as pd
from strategies import BaseStrategy, register

@register('chaos_breakout')
class ChaosBreakoutStrategy(BaseStrategy):
    """
    Реализация стратегии Chaos Breakout (Bill Williams):
    – Использует Alligator, Fractals, Awesome Oscillator (AO), Accelerator Oscillator (AC).
    – Вход в лонг: цена пробила последний верхний фрактал, lips>teeth>jaw, AO>0, AC>0
    – Вход в шорт: цена пробила последний нижний фрактал, lips<teeth<jaw, AO<0, AC<0

    Аллигатор (Alligator)
        Индикатор состоит из трёх линий:
            jaw (челюсть Аллигатора):
                Период длинного скользящего среднего. По умолчанию jaw = 13.
                Чем больше число, тем более плавная и медленная линия.
            teeth (зубы Аллигатора):
                Среднее скользящее. По умолчанию teeth = 8.
                Определяет среднесрочные движения цены.
            lips (губы Аллигатора):
                Короткое скользящее среднее. По умолчанию lips = 5.
                Чем меньше число, тем чувствительнее сигнал к краткосрочным изменениям.

        Сдвиги линий Аллигатора (Shifts)
        Сдвиги определяют, насколько линии Аллигатора смещаются вперёд относительно текущей свечи (бара):
            jaw_shift (по умолчанию jaw_shift = 8)
            teeth_shift (по умолчанию teeth_shift = 5)
            lips_shift (по умолчанию lips_shift = 3)
    Совет:
        Увеличение сдвига делает сигналы более консервативными (позже вход, меньше ложных сигналов).
        Уменьшение сдвига увеличивает чувствительность и скорость сигналов.
    Awesome Oscillator (AO)
        AO сравнивает короткий и длинный период скользящих средних медианной цены.
            ao_fast (по умолчанию ao_fast = 5)
            ao_slow (по умолчанию ao_slow = 34)
        Совет:
            Сокращение периодов увеличивает чувствительность AO (больше сигналов, но чаще ложные).
            Удлинение периодов делает AO более стабильным, но сигналы более редкие.
    Accelerator Oscillator (AC)
        AC показывает ускорение или замедление импульса цены относительно AO:
            ac_slow (по умолчанию ac_slow = 5)
        Совет:
            Увеличение периода делает AC медленнее (снижает количество сигналов).
            Уменьшение периода ускоряет AC (увеличивает число сигналов).
    Фракталы (Fractals)
        Фракталы используются для определения уровней пробоя.
            fractal_window (по умолчанию fractal_window = 5, должно быть нечетным числом)
        Совет:
            Увеличение окна (например, до 7 или 9) делает фракталы сильнее, но реже.
            Уменьшение окна (например, до 3) увеличивает чувствительность, но может привести к ложным сигналам.

    Управление рисками (Position sizing)
        risk_target (по умолчанию risk_target = 0.01 – это 1% от капитала на каждую сделку)
        Совет:
            Уменьшение (0.005) снижает риски и просадки.
            Увеличение (0.02 и выше) увеличивает потенциальную доходность, но повышает риски и волатильность капитала.

    Рекомендуемый порядок экспериментов:
        Начни с игры с параметрами Аллигатора и его сдвигами.
        Они оказывают сильное влияние на скорость и качество сигналов.
        Затем AO и AC, если хочешь изменить чувствительность к импульсам рынка.
        Далее настрой фракталы, если тебе нужны более явные пробои или больше сигналов.
        В конце настрой уровень риска, подбирая оптимальный баланс доходности и просадки.
    """
    def __init__(
        self,
        jaw: int = 13, teeth: int = 8, lips: int = 5,
        jaw_shift: int = 8, teeth_shift: int = 5, lips_shift: int = 3,
        ao_fast: int = 5, ao_slow: int = 34, ac_slow: int = 5,
        fractal_window: int = 5,
        risk_target: float = 0.01
    ):
        self.jaw = jaw
        self.teeth = teeth
        self.lips = lips
        self.jaw_shift = jaw_shift
        self.teeth_shift = teeth_shift
        self.lips_shift = lips_shift
        self.ao_fast = ao_fast
        self.ao_slow = ao_slow
        self.ac_slow = ac_slow
        self.fractal_w = fractal_window
        self.risk = risk_target

    def smma(self, series, period):
        return series.ewm(alpha=1/period, adjust=False).mean()

    def _fractals(self, high: pd.Series, low: pd.Series):
        shift = self.fractal_w // 2
        conditions_high = pd.Series(True, index=high.index)
        conditions_low = pd.Series(True, index=low.index)

        for i in range(-shift, shift + 1):
            if i == 0:
                continue
            conditions_high &= high > high.shift(i)
            conditions_low &= low < low.shift(i)

        return conditions_high, conditions_low

    def _awesome(self, high: pd.Series, low: pd.Series) -> pd.Series:
        median_price = (high + low) / 2
        ao = median_price.rolling(self.ao_fast).mean() - median_price.rolling(self.ao_slow).mean()
        return ao

    def _ac(self, ao: pd.Series) -> pd.Series:
        return ao - ao.rolling(self.ac_slow).mean()

    def _single(self, df: pd.DataFrame) -> pd.Series:
        price = df['Close']
        high = df['High']
        low = df['Low']

        jaw_line = self.smma(price.shift(self.jaw_shift), self.jaw)
        teeth_line = self.smma(price.shift(self.teeth_shift), self.teeth)
        lips_line = self.smma(price.shift(self.lips_shift), self.lips)

        is_high, is_low = self._fractals(high, low)
        last_high = high.where(is_high).ffill().shift(1)
        last_low = low.where(is_low).ffill().shift(1)

        ao = self._awesome(high, low)
        ac = self._ac(ao)

        long_break = price > last_high
        short_break = price < last_low

        all_long = (lips_line > teeth_line) & (teeth_line > jaw_line)
        all_short = (lips_line < teeth_line) & (teeth_line < jaw_line)

        long_signal = long_break & all_long & (ao > 0) & (ac > 0)
        short_signal = short_break & all_short & (ao < 0) & (ac < 0)

        pos = long_signal.astype(float) - short_signal.astype(float)
        return (pos * self.risk).fillna(0)

    def generate(self, df: pd.DataFrame) -> pd.DataFrame:
        result = pd.DataFrame(index=df.index)
        syms = df.columns.get_level_values(0).unique()
        for sym in syms:
            intraday = df[sym]
            if len(intraday) < self.fractal_w:
                result[sym] = 0
            else:
                result[sym] = self._single(intraday)
        return result.fillna(0)