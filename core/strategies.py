import pandas as pd
from common.property_reader import PropertyReader
from common.utilities import Utilities
import json
from datetime import datetime, timedelta
from pathlib import Path
from datetime import date


class Strategies:
    def __init__(self, prop_file):
        self.p_reader = PropertyReader(prop_file=prop_file)
        self.utilities = Utilities()

    def strategy_supertrend_ema100(self, df):
        df = self.filter_by_date(df)
        df1 = df[((df['buy_short'] > 0.0) & (df['close'] > df['ema100'])) | (
                (df['sell_short'] > 0.0) & (df['close'] < df['ema100']))]

        cols = ['ticker', 'timestamp', 'close', 'sup_short', 'ema100', 'buy_short', 'sell_short']
        df1 = df1[cols]
        buys = df1[df1['buy_short'] > 0.0]
        sells = df1[df1['sell_short'] > 0.0]
        return buys, sells

    def strategy_supertrend_ema55(self, df):
        df = self.filter_by_date(df)
        df1 = df[((df['buy_short'] > 0.0) & (df['close'] > df['ema55'])) | (
                (df['sell_short'] > 0.0) & (df['close'] < df['ema55']))]

        cols = ['ticker', 'timestamp', 'close', 'sup_short', 'ema55', 'buy_short', 'sell_short']
        df1 = df1[cols]
        buys = df1[df1['buy_short'] > 0.0]
        sells = df1[df1['sell_short'] > 0.0]
        return buys, sells

    def strategy_supertrend_short_medium(self, df):
        df = self.filter_by_date(df)
        df1 = df[((df['buy_short'] > 0.0) & (df['buy_medium'] > 0.0)) | (
                (df['sell_short'] > 0.0) & (df['sell_medium'] > 0.0))]

        cols = ['ticker', 'timestamp', 'close', 'sup_short', 'sup_medium', 'buy_short', 'sell_short']
        df1 = df1[cols]
        buys = df1[df1['buy_short'] > 0.0]
        sells = df1[df1['sell_short'] > 0.0]
        return buys, sells

    def strategy_supertrend_short_long(self, df):
        df = self.filter_by_date(df)
        df1 = df[((df['buy_short'] > 0.0) & (df['buy_long'] > 0.0)) | (
                (df['sell_short'] > 0.0) & (df['sell_long'] > 0.0))]

        cols = ['ticker', 'timestamp', 'close', 'sup_short', 'sup_long', 'buy_short', 'sell_short']
        df1 = df1[cols]
        buys = df1[df1['buy_short'] > 0.0]
        sells = df1[df1['sell_short'] > 0.0]
        return buys, sells

    def filter_by_date(self, df) -> pd.DataFrame:
        filter_by_intervals = self.p_reader.last_interval + 1
        filter_by_date = datetime.today() - timedelta(days=filter_by_intervals)
        df = df[df['date'] >= filter_by_date]
        return df

    def read_indicator_file(self) -> pd.DataFrame:
        _dir = self.p_reader.historical_dir
        _startswith = self.p_reader.indicator_startswith
        latest_file = self.utilities.get_latest_file(directory=_dir, startswith=_startswith)

        data = list()
        with open(latest_file, 'r') as handler:
            for line in handler:
                js = json.loads(line)
                data.append(js)
        df = pd.DataFrame(data)
        df = df.drop_duplicates(keep='first')
        return df

    def create_df_output_file(self, df:pd.DataFrame) -> str:
        _dir = self.p_reader.historical_dir
        _startswith = self.p_reader.strategy_startswith
        file_path = Path(_dir + '/' + _startswith + date.today().strftime("%Y_%m_%d"))
        records = df.to_dict('records')
        writer = open(str(file_path.absolute()), 'w')
        for record in records:
            writer.write(json.dumps(record))
            writer.write('\n')
        writer.close()
        return str(file_path.absolute())

    def create_buy_sell_output_file(self, buys, sells):
        _dir = self.p_reader.historical_dir
        _startswith = self.p_reader.buysell_startswith
        file_path = Path(_dir + '/' + _startswith + date.today().strftime("%Y_%m_%d"))

        writer = open(str(file_path.absolute()), 'w')
        writer.write(json.dumps({'BUY':list(buys), 'SELL':list(sells)}))
        writer.close()
        return

    def apply_strategies(self):
        df = self.read_indicator_file()
        df['date'] = pd.to_datetime(df.timestamp, format="%Y %m %d %H:%M:%S")

        buy_sup_short_medium, sell_sup_short_medium = self.strategy_supertrend_short_medium(df)
        buy_sup_short_long, sell_sup_short_long = self.strategy_supertrend_short_long(df)
        buy_sup_short_ema55, sell_sup_short_ema55 = self.strategy_supertrend_ema55(df)
        buy_sup_short_ema100, sell_sup_short_ema100 = self.strategy_supertrend_ema100(df)

        # all list of stocks to file
        all_buys = set(list(buy_sup_short_ema55['ticker']) + list(buy_sup_short_ema100['ticker']) + list(
            buy_sup_short_long['ticker']) + list(buy_sup_short_medium['ticker']))
        all_sells = set(list(sell_sup_short_ema55['ticker']) + list(sell_sup_short_ema100['ticker']) + list(
            sell_sup_short_long['ticker']) + list(sell_sup_short_medium['ticker']))
        self.create_buy_sell_output_file(all_buys, all_sells)

        # save all filtered stocks to a file
        frames = [buy_sup_short_medium, sell_sup_short_medium, buy_sup_short_long, sell_sup_short_long,
                  buy_sup_short_ema55, sell_sup_short_ema55, buy_sup_short_ema100, sell_sup_short_ema100]
        strategy_df = pd.concat(frames)
        strategy_df = strategy_df.drop_duplicates(keep='first')
        self.create_df_output_file(strategy_df)
        return


# TODO: add Pankaj's Strategy
"""
## Pankaj Strategy
interval: 10 min
buy: price above 20 sma (bollinger) and above previous closest high 
	strong buy: price above vwap
sell: price below 5 ema + 1 interval
leverage ??
"""

# TODO - add https://www.youtube.com/watch?v=KO7lX7-Fi7U strategy
