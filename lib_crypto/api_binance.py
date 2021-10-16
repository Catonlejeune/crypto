import pandas as pd
import requests
import datetime
import logging
import lib_Utils.sql_module as sql


class BinanceApi:
    """
    Class to get data from the API Binance
    """

    def __init__(self):
        """
        Initalisation of the Binance Api object
        """
        self.logger = logging.getLogger(self.__class__.__name__ + "_Basic_Logger")
        self.logger_file_handler = logging.FileHandler("log_api_binance.log")
        self.logger_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        self.logger_file_handler.setFormatter(self.logger_formatter)
        self.logger.addHandler(self.logger_file_handler)

        # To rename columns in function
        self.columns = {0: 'Open_time', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close', 5: 'Volume', 6: 'Close_time',
                        7: 'Quote_asset_volume', 8: 'Number_of_trades', 9: 'Taker_buy_base_asset_volume',
                        10: 'Taker_buy_quote_asset_volume', 11: 'Ignore'}

    def get_data_spot_data(self, code, interval, date_debut=None, date_fin=datetime.datetime(2018, 1, 1)):
        """
        Get data from Binance api
        :param code: str for instance 'BTCETH'
        :param interval: str '1m', '1h' etc
        :param date_debut: datetime
        :param date_fin: datetime
        :return: None, insertion into BDD
        """
        # Creation of a dataframe which will contain all data.
        if date_debut:
            date = date_debut
        else:
            date = datetime.datetime(datetime.datetime.today().year, datetime.datetime.today().month,
                                     datetime.datetime.today().day)
        # loop to laod code data from date_debut to date_fin
        while date >= date_fin:
            try:
                # recurences on the dates
                date_t_1 = int((date - datetime.datetime(1970, 1, 1)).total_seconds()) * 1000
                date_t_0 = int(
                    (date - datetime.timedelta(days=1) - datetime.datetime(1970, 1, 1)).total_seconds()) * 1000
                date = date - datetime.timedelta(days=1)

                # API request
                req = requests.get(
                    f'https://api.binance.com/api/v3/klines?symbol={code}&interval={interval}&startTime={date_t_0}&endTime={date_t_1}')

                # To change json response to a dataframe
                df_insert_cryptofiat_df = pd.DataFrame(req.json()).rename(columns=self.columns)

                # To set up a good data format
                df_insert_cryptofiat_df['Open_time'] = pd.to_datetime(df_insert_cryptofiat_df['Open_time'] / 1000,
                                                                      unit='s')
                df_insert_cryptofiat_df['Close_time'] = pd.to_datetime(df_insert_cryptofiat_df['Close_time'] / 1000,
                                                                       unit='s')
                df_insert_cryptofiat_df.columns = df_insert_cryptofiat_df.columns.str.lower()
                df_insert_cryptofiat_df['code'] = code
                df_insert_cryptofiat_df['interval_sample'] = interval
                df_insert_cryptofiat_df.rename(columns={'open': 'open_price', 'high': 'high_price',
                                                        'low': 'low_price', 'close': 'close_price',
                                                        'ignore': 'ignore_bool'}, inplace=True)
                # Concat data in one dataframe
                df_insert_cryptofiat_df.drop_duplicates(subset=['open_time', 'code', 'interval_sample'],
                                                        inplace=True)
                df_code = df_insert_cryptofiat_df[['code']].drop_duplicates(subset=['code'])
                df_code['source'] = 'Binance'
                sql.insert_update_sql(df_code,
                                      table='defcodecrypto',
                                      primary_key=['Code'],
                                      do_update=False)

                sql.insert_update_sql(df_insert_cryptofiat_df,
                                      table='cryptotfiat_df',
                                      primary_key=['Open_time', 'Close_time', 'Code', 'Interval'],
                                      do_update=True)
                self.logger.info(f'##### Insertion suceed #####')
                print('##### Insertion suceed #####')
                self.logger.info(f'Done : {date}, {code}')
                print(f'Done : {date}, {code}')
            except Exception as e:
                print(f'Error : {date}, {code}, {e}')
                self.logger.error(f'Error : {date}, {code}, {e}')
                print('##### Insertion failed #####')

    def run(self, update=False):
        if update:
            date_fin = datetime.datetime.today() - datetime.timedelta(days=30)
        else:
            date_fin = datetime.datetime(2020, 1, 1)
        self.get_data_spot_data('ETHTUSD', '1m', date_debut=datetime.datetime.today(),
                                date_fin=date_fin)
        self.get_data_spot_data('BTCTUSD', '1m', date_debut=datetime.datetime.today(),
                                date_fin=date_fin)

        self.get_data_spot_data('BNBTUSD', '1m', date_debut=datetime.datetime.today(),
                                date_fin=date_fin)

        self.get_data_spot_data('DOGETUSD', '1m', date_debut=datetime.datetime.today(),
                                date_fin=date_fin)

        self.get_data_spot_data('XRPTUSD', '1m', date_debut=datetime.datetime.today(),
                                date_fin=date_fin)

        self.get_data_spot_data('ADATUSD', '1m', date_debut=datetime.datetime.today(),
                                date_fin=date_fin)

        self.get_data_spot_data('SOLTUSD', '1m', date_debut=datetime.datetime.today(),
                                date_fin=date_fin)


def run():
    binance_api = BinanceApi()
    binance_api.run(update=True)


if __name__ == '__main__':
    run()
