import mgdb as mgdb
import pandas as pd
import matplotlib.pyplot as plt


def plotting_graphics():
    df = mgdb.read_mongo('crypto_spot_database', "", "1m", conn=None)
    df.Open_time = df.Open_time.dt.strftime('%Y%m%d')
    df.Open = pd.to_numeric(df.Open)
    df.Volume = pd.to_numeric(df.Volume)
    df_graphics = df.groupby(by=['Open_time', 'Ssjacent'])[['Open', 'Volume']].mean().reset_index()
    for idx, df_ssjacent in df_graphics.groupby(by='Ssjacent'):
        df_ssjacent.Open_time = pd.to_datetime(df_ssjacent.Open_time, format='%Y%m%d')
        df_ssjacent.sort_values(by='Open_time', inplace=True)
        fig, ax = plt.subplots()
        plt.title('Open price: ' + str(idx))
        ax.plot(df_ssjacent.Open_time, df_ssjacent.Open)
        ax.set_xlabel('date')
        ax.set_ylabel('price BUSD')
        ax2 = ax.twinx()
        ax2.plot(df_ssjacent.Open_time, df_ssjacent.Volume, color='r')
        ax2.set_ylabel('Volume')
        plt.setp(ax.get_xticklabels(), rotation=45, horizontalalignment='right')
        plt.show()
