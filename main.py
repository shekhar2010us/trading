from core.data_collector import DataDownloader
from core.indicators import IndicatorCalculator
from core.strategies import Strategies


def data_downloader(prop_file):
    print('Data Downloading starts..')
    downloader = DataDownloader(prop_file)
    downloader.download_data()
    print('Data Downloading done..')


def indicator_calculator(prop_file):
    print('\nIndicator calculations starts..')
    indicator_calc = IndicatorCalculator(prop_file)
    indicator_calc.calculate_indicators()
    print('Indicator calculations done..')


def strategy_runner(prop_file):
    print('\nStrategy application starts..')
    strategy_maker = Strategies(prop_file=prop_file)
    strategy_maker.apply_strategies()
    print('Strategy application done..')


if __name__ == '__main__':
    prop_file = '/Users/shekagra/Documents/dev/team-per/trader/data/conf.ini'
    data_downloader(prop_file)
    indicator_calculator(prop_file)
    strategy_runner(prop_file)
