import math
import warnings
import numpy as np
import pandas as pd
from pandas import DataFrame
import time
import yfinance as yf
from datetime import date
from pathlib import Path
import json
import sys
from common.property_reader import PropertyReader
warnings.filterwarnings("ignore")


class TickerFileReader:
    def __init__(self, ticker_file):
        self.ticker_file = ticker_file

    def read_ticker_file(self) -> dict:
        tickers = dict()
        li = None
        ticker_domain = None
        with open(self.ticker_file, 'r') as handler:
            for line in handler:
                line = line.replace('\n', '').strip()
                if line.startswith('<'):
                    if li is not None and ticker_domain is not None:
                        tickers[ticker_domain] = li
                    ticker_domain = line[1:-1]
                    li = list()
                elif li is not None:
                    li.append(line)
            tickers[ticker_domain] = li
        return tickers

    def debug_tickers(self, tickers:dict):
        n=0
        for k,v in tickers.items():
            print(k, len(v))
            n += len(v)
        print('total tickers', n)


class DataCollector:
    def __init__(self, interval, period, use_combine_interval,
                 combine_interval, start_date, end_date):
        self.interval = interval
        self.period = period
        self.use_combine_interval = use_combine_interval
        self.combine_interval = combine_interval
        self.start_date = start_date
        self.end_date = end_date

    def __get_stock_history_period(self, name):
        df = yf.download(tickers=name, period=self.period, interval=self.interval)
        df.reset_index(inplace=True)
        df = df.rename(columns={'index': 'Date'})
        return df

    def __get_stock_history_dates(self, name):
        ticker = yf.Ticker(name)
        df = ticker.history(interval=self.interval, start=self.start_date, end=self.end_date)
        df.reset_index(inplace=True)
        df = df.rename(columns={'index': 'Date'})
        return df

    def __interval_multiplier_df_creator(self, df):
        '''
        works only
           - when input `df` is 1-h; output needed is 2-h, 3-h, 4-h
           - when input `df` is 1-m; output needed is 2-m, 3-m, 4-m
        '''
        num_chunks = math.ceil(df.shape[0] / self.combine_interval)

        _list = list()
        for ch in np.array_split(df, num_chunks):
            _list.append(ch)

        dat = list()
        for ch in _list:
            _date = ch['Date'].iloc[0]
            _open = ch['Open'].iloc[0]
            _close = ch['Close'].iloc[-1]
            _high = ch['High'].max()
            _low = ch['Low'].min()
            _vol = ch['Volume'].sum() / ch.shape[0]
            dat.append((_date, _open, _high, _low, _close, _vol))

        cols = ["Date", "Open", "High", "Low", "Close", "Volume"]
        df_new = pd.DataFrame(data=dat, columns=cols)
        return df_new

    def get_data(self, name:list) -> DataFrame:
        '''
        name = 'AAPL'
        start = '2021-08-01'
        end = '2021-12-21'
        interval = '1h', '1d'
        period = '30d', '10d'
        '''
        '''
            1d, 90d
            1h, 15d
            2h, 25d
            4h, 50d
        '''
        df = None
        if name is None or len(name)==0 or self.interval is None or len(self.interval.strip())==0:
            return None
        if self.period is not None and len(self.period.strip()) > 0:
            df = self.__get_stock_history_period(name=name)
        elif self.start_date is not None and self.end_date is not None \
                and len(self.start_date.strip()) > 0 and len(self.end_date.strip()) > 0:
            df = self.__get_stock_history_dates(name=name)

        if self.use_combine_interval and self.combine_interval > 0:
            df = self.__interval_multiplier_df_creator(df)

        return df

    def convert_data_list(self, df:DataFrame) -> list:
        out = list()
        dicts = df.to_dict('records')
        for line in dicts:
            timestamp = line['Date'].strftime("%Y %m %d %H:%M:%S")
            _open = line['Open'].to_dict()
            _close = line['Close'].to_dict()
            _high = line['High'].to_dict()
            _low = line['Low'].to_dict()
            _vol = line['Volume'].to_dict()
            keys = _open.keys()

            for key in keys:
                record = {'ticker': key,
                          'timestamp': timestamp,
                          'open': _open[key],
                          'close': _close[key],
                          'high': _high[key],
                          'low': _low[key],
                          'volume': _vol[key]}
                out.append(record)
        return out

    def create_output_file(self, directory:str, startswith:str) -> str:
        try:
            file_path = Path(directory + '/' + startswith + date.today().strftime("%Y_%m_%d"))
            file_path.touch(exist_ok=True)
            filename = str(file_path.absolute())
        except Exception as e:
            print(e)
            return None
        return filename

    def save_output(self, filename:str, records:list):
        writer = open(filename, 'a')
        for record in records:
            writer.write(json.dumps(record))
            writer.write('\n')
        writer.close()


class DataDownloader:
    # TODO - save data for 30m, 1h, 2h, 4h, 1d - in different files
    def __init__(self, prop_file):
        self.p_reader = PropertyReader(prop_file=prop_file)

    def read_ticker_file(self) -> dict:
        ticker_file_reader = TickerFileReader(ticker_file=self.p_reader.ticker_file)
        tickers = ticker_file_reader.read_ticker_file()
        return tickers

    def download_data(self) -> int:
        tickers = self.read_ticker_file()
        data_collector = DataCollector(interval=self.p_reader.interval,
                                       period=self.p_reader.period,
                                       use_combine_interval=self.p_reader.use_combine_interval,
                                       combine_interval=self.p_reader.combine_interval,
                                       start_date=self.p_reader.start_date,
                                       end_date=self.p_reader.end_date)
        # create blank output file
        _dir = self.p_reader.historical_dir
        _startswith = self.p_reader.history_startswith
        o_filename = data_collector.create_output_file(directory=_dir, startswith=_startswith)
        print('Data Downloader Output File:', o_filename)
        if o_filename is None:
            sys.exit(-1)

        # download and save data
        for category, ticker_list in tickers.items():
            time.sleep(2)
            print(category,'>>',len(ticker_list))
            df = data_collector.get_data(name=ticker_list)
            records = data_collector.convert_data_list(df)
            data_collector.save_output(filename=o_filename, records=records)
        return 1
