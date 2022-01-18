import configparser
import ast


class PropertyReader:

    def __init__(self, prop_file):
        config = configparser.ConfigParser()
        config.read(prop_file)

        self.interval = str(config.get('Data', 'loader.interval'))
        self.period = str(config.get('Data', 'loader.period'))
        self.use_combine_interval = bool(config.get('Data', 'loader.use_combine_interval'))
        self.combine_interval = int(config.get('Data', 'loader.combine_interval'))
        self.start_date = str(config.get('Data', 'loader.start_date'))
        self.end_date = str(config.get('Data', 'loader.end_date'))
        self.ticker_file = str(config.get('Data', 'loader.ticker.file'))
        self.historical_dir = str(config.get('Data', 'saver.dir'))
        self.history_startswith = str(config.get('Data', 'saver.dir.history'))
        self.indicator_startswith = str(config.get('Data', 'saver.dir.indicators'))
        self.strategy_startswith = str(config.get('Data', 'saver.dir.strategy'))
        self.buysell_startswith = str(config.get('Data', 'saver.dir.buysells'))

        self.emas = ast.literal_eval(config.get('Indicators', 'indicator.emas'))
        self.supertrends = ast.literal_eval(config.get('Indicators', 'indicator.supertrends'))

        self.last_interval = int(config.get('Strategy', 'strategy.last.intervals'))