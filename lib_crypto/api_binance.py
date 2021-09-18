import pandas as pd
import requests
import datetime
import logging
import lib_Utils.sql_module as sql

class BinanceScraper:
    """
    Class to get data from the API Binance
    """
    def __init__(self):
        self.df_insert_cryptofiat_df = pd.DataFrame()
        self.df_code = pd.DataFrame()
        self.logger = logging.getLogger(self.__class__.__name__ + "_Basic_Logger")
        self.logger_file_handler = logging.FileHandler("log_api_binance.log")
        self.logger_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        self.logger_file_handler.setFormatter(self.logger_formatter)
        self.logger.addHandler(self.logger_file_handler)

        #To rename columns in function
        self.columns = {0: 'Open_time', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close', 5: 'Volume', 6: 'Close_time',
                        7: 'Quote_asset_volume', 8: 'Number_of_trades', 9: 'Taker_buy_base_asset_volume',
                        10: 'Taker_buy_quote_asset_volume', 11: 'Ignore'}

    # get historical data for spot product OPHC
    # @Input : Scraper object.
    #          Code product.
    #          frequency interval ('1s','1h' etc cf binance)
    #          date_debut, datetime object.
    #          date_fin, datetime object.
    # @Output : CSV with all data for one code
    def get_data_spot_data(self, code, interval, date_debut=None, date_fin=datetime.datetime(2018, 1, 1)):
        # Creation of a dataframe which will contain all data.
        if date_debut:
            date = date_debut
        else:
            date = datetime.datetime(datetime.datetime.today().year, datetime.datetime.today().month,
                                     datetime.datetime.today().day)
        #loop to laod code data from date_debut to date_fin
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
                df = pd.DataFrame(req.json()).rename(columns=self.columns)

                #To set up a good data format
                df['Open_time'] = pd.to_datetime(df['Open_time'] / 1000, unit='s')
                df['Close_time'] = pd.to_datetime(df['Close_time'] / 1000, unit='s')
                df['Code'] = code
                df['Interval'] = interval
                #Concat data in one dataframe
                self.df_insert_cryptofiat_df = pd.concat([self.df_insert_cryptofiat_df, df])
                self.logger.info(f'Done : {date}, {code}')
                print(f'Done : {date}, {code}')
            except Exception as e:
                print(f'Error : {date}, {code}, {e}')
                self.logger.error(f'Error : {date}, {code}, {e}')
        try:
            # To push data
            self.df_insert_cryptofiat_df.drop_duplicates(subset=['Open_time', 'Code','Interval'], inplace=True)
            self.df_code = self.df_insert_cryptofiat_df[['Code']].drop_duplicates(subset=['Code'])
            self.df_code['Source'] = 'Binance'
            sql.insert_update_sql(self.df_code,
                                  table='defcodecrypto',
                                  primary_key=['Code'],
                                  do_update=False)

            sql.insert_update_sql(self.df_insert_cryptofiat_df,
                                  table='cryptotfiat_df',
                                  primary_key=['Open_time','Close_time','Code'],
                                  do_update=True)
            # mongodbpush.push_pandas_mongodb(result, table='crypto_spot_database')
            self.logger.info(f'##### Insertion suceed #####')
            print('##### Insertion suceed #####')

        except Exception as e:
            print(f'Error :  {e}')
            self.logger.error(f'Error : {e}')
            print('##### Insertion failed #####')



    # A finir  https://binance-docs.github.io/apidocs/futures/en/#continuous-contract-kline-candlestick-data
    # Endpoint not good
    def run(self,update=False):
        date_fin=datetime.datetime(2021, 9, 1)
        self.get_data_spot_data('ETHBUSD', '1m', date_debut=datetime.datetime.today(),
                                           date_fin=date_fin)

        self.get_data_spot_data('BNBBUSD', '1m', date_debut=datetime.datetime.today(),
                                          date_fin=date_fin)

        self.get_data_spot_data('DOGEBUSD', '1m', date_debut=datetime.datetime.today(),
                                           date_fin=date_fin)

        self.get_data_spot_data('XRPBUSD', '1m', date_debut=datetime.datetime.today(),
                                       date_fin=date_fin)


def run():
    binance_api = BinanceScraper()
    binance_api.run()


if __name__ == '__main__':
    run()
