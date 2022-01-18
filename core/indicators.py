import numpy as np
import pandas as pd
import pandas_ta as ta
import warnings
import json
from datetime import date
from pathlib import Path
from common.utilities import Utilities
from common.property_reader import PropertyReader
warnings.filterwarnings("ignore")


class Indicators:
    def __init__(self):
        return

    def __is_support(self, df:pd.DataFrame, i:int):
        support = df['Low'][i] < df['Low'][i-1]  and df['Low'][i] < df['Low'][i+1] \
                  and df['Low'][i+1] < df['Low'][i+2] and df['Low'][i-1] < df['Low'][i-2]
        return support

    def __is_resistance(self, df:pd.DataFrame, i:int):
        resistance = df['High'][i] > df['High'][i-1]  and df['High'][i] > df['High'][i+1] \
                     and df['High'][i+1] > df['High'][i+2] and df['High'][i-1] > df['High'][i-2]
        return resistance

    def __is_far_from_level(self, l, levels, s):
        return np.sum([abs(l-x) < s for x in levels]) == 0

    def __calculate_levels(self, df:pd.DataFrame):
        support = []
        resistance = []
        levels = []
        s = np.mean(df['High'] - df['Low'])
        for i in range(2, df.shape[0]-2):
            if self.__is_support(df, i):
                l = df['Low'][i]
                if self.__is_far_from_level(l, levels, s):
                    levels.append((i, l))
                    support.append((i, l))
            elif self.__is_resistance(df, i):
                l = df['High'][i]
                if self.__is_far_from_level(l, levels, s):
                    levels.append((i, l))
                    resistance.append((i, l))
        return levels, support, resistance

    def calculate_ema(self, df1:pd.DataFrame, ema:int, col_name:str='close') -> pd.DataFrame:
        df1['ema' + str(ema)] = df1[col_name].ewm(span=ema, min_periods=0, adjust=False, ignore_na=False).mean()
        return df1

    def calculate_supertrend(self, df_t:pd.DataFrame, identifier:str, period:int, multiplier:int):
        r_col = 'SUPERT_7_' + str(int(multiplier)) + '.0'
        l_col = 'sup_' + identifier
        df_t[l_col] = ta.supertrend(high=df_t['high'], low=df_t['low'], close=df_t['close'], period=period, multiplier=multiplier)[r_col]

        df_t['buy_' + identifier] = 0.0
        df_t['sell_' + identifier] = 0.0
        n = period - 1
        for i in range(n, len(df_t)):
            if df_t['close'][i - 1] <= df_t['sup_' + identifier][i - 1] and df_t['close'][i] > \
                    df_t['sup_' + identifier][i]:
                df_t['buy_' + identifier][i] = 1.0
            if df_t['close'][i - 1] >= df_t['sup_' + identifier][i - 1] and df_t['close'][i] < \
                    df_t['sup_' + identifier][i]:
                df_t['sell_' + identifier][i] = 1.0
        return df_t


class IndicatorCalculator:
    def __init__(self, prop_file):
        self.p_reader = PropertyReader(prop_file=prop_file)
        self.indicators = Indicators()
        self.utilities = Utilities()

    def read_history_file(self) -> pd.DataFrame:
        _dir = self.p_reader.historical_dir
        _startswith = self.p_reader.history_startswith
        latest_file = self.utilities.get_latest_file(directory=_dir, startswith=_startswith)

        data = list()
        with open(latest_file, 'r') as handler:
            for line in handler:
                js = json.loads(line)
                data.append(js)
        df = pd.DataFrame(data)
        df = df.drop_duplicates(keep='first')
        return df

    def create_ouput_file(self, df:pd.DataFrame) -> str:
        _dir = self.p_reader.historical_dir
        _startswith = self.p_reader.indicator_startswith
        file_path = Path(_dir + '/' + _startswith + date.today().strftime("%Y_%m_%d"))
        records = df.to_dict('records')
        writer = open(str(file_path.absolute()), 'w')
        for record in records:
            writer.write(json.dumps(record))
            writer.write('\n')
        writer.close()
        return str(file_path.absolute())

    def ema_supertrend_calculator(self, df:pd.DataFrame) -> pd.DataFrame:
        tickers = set(df['ticker'])

        temp = list()
        for ticker in tickers:
            df1 = df[df['ticker'] == ticker]
            df1 = df1.reset_index()

            for ema in self.p_reader.emas:
                df1 = self.indicators.calculate_ema(df1, ema=ema, col_name='close')
            for sup in self.p_reader.supertrends:
                period = sup[0]
                multiplier = sup[1]
                identifier = sup[2]
                df1 = self.indicators.calculate_supertrend(df_t=df1, identifier=identifier, period=period, multiplier=multiplier)
            temp.append(df1)
        df_indicator = pd.concat(temp)
        return df_indicator

    def calculate_indicators(self):
        df = self.read_history_file()
        df = self.ema_supertrend_calculator(df)
        # TODO - add support resistance levels
        self.create_ouput_file(df)
        return
