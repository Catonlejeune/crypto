import pandas as pd
import Utils.mgdb as mongodbpush
import requests
import datetime
import logging


class BinanceScraper:
    def __init__(self):

        # Definir log fil

        self.columns = {0: 'Open_time', 1: 'Open', 2: 'High', 3: 'Low', 4: 'Close', 5: 'Volume', 6: 'Close_time',
                        7: 'Quote_asset_volume', 8: 'Number_of_trades', 9: 'Taker_buy_base_asset_volume',
                        10: 'Taker_buy_quote_asset_volume', 11: 'Ignore'}

    # get historical date for spot product OPHC
    # @Input : Scraper object.
    #          Code product.
    #          frequency interval ('1s','1h' etc cf binance)
    #          date_debut, datetime object.
    #          date_fin, datetime object.
    # @Output : None
    def get_historical_date_spot_data(self, code, interval, date_debut=None, date_fin=datetime.datetime(2018, 1, 1)):
        result = pd.DataFrame()
        if date_debut:
            date = date_debut
        else:
            date = datetime.datetime(datetime.datetime.today().year, datetime.datetime.today().month,
                                     datetime.datetime.today().day)
        while date >= date_fin:
            try:
                date_t_1 = int((date - datetime.datetime(1970, 1, 1)).total_seconds()) * 1000
                date_t_0 = int(
                    (date - datetime.timedelta(days=1) - datetime.datetime(1970, 1, 1)).total_seconds()) * 1000
                date = date - datetime.timedelta(days=1)
                req = requests.get(
                    f'''https://api.binance.com/api/v3/klines?
                    symbol={code}
                    &interval={interval}
                    &startTime={date_t_0}
                    &endTime={date_t_1}''')

                # On peut l'envoyer direct en json mais je voulais faire une modifie sur les dates
                df = pd.DataFrame(req.json()).rename(columns=self.columns)
                df['Open_time'] = pd.to_datetime(df['Open_time'] / 1000, unit='s')
                df['Close_time'] = pd.to_datetime(df['Close_time'] / 1000, unit='s')
                df['interval'] = interval
                result = pd.concat([result, df])
                print('Done : ', date)
            except Exception as e:
                print('Error : ', e)
                print(df)
        try:
            result['Ssjacent'] = code
            df['Interval'] = interval
            result.drop_duplicates(subset=['Open_time', 'Spot'], inplace=True)
            mongodbpush.push_pandas_mongodb(result, table='crypto_spot_database')
            print('##### Insertion suceed #####')
        except Exception as e:
            print(e)
            print('##### Insertion failed #####')

    def update(self):
        pass

    # A finir  https://binance-docs.github.io/apidocs/futures/en/#continuous-contract-kline-candlestick-data
    # Endpoint not good
    def run(self):
        self.get_historical_date_spot_data('BTCBUSD', '1s', date_debut=datetime.datetime.today(),
                                           date_fin=datetime.datetime(2019, 1, 1))
        self.get_historical_date_spot_data('ETHBUSD', '1s', date_debut=datetime.datetime.today(),
                                           date_fin=datetime.datetime(2019, 1, 1))
        self.get_historical_date_spot_data('ETH2BUSD', '1s', date_debut=datetime.datetime.today(),
                                           date_fin=datetime.datetime(2019, 1, 1))
        self.get_historical_date_spot_data('BNBBUSD', '1s', date_debut=datetime.datetime.today(),
                                           date_fin=datetime.datetime(2019, 1, 1))
        self.get_historical_date_spot_data('DOGEBUSD', '1s', date_debut=datetime.datetime.today(),
                                           date_fin=datetime.datetime(2019, 1, 1))
        self.get_historical_date_spot_data('XRPBUSD', '1s', date_debut=datetime.datetime.today(),
                                           date_fin=datetime.datetime(2019, 1, 1))


def run():
    binance_api = BinanceScraper()
    binance_api.run()


if __name__ == '__main__':
    run()
