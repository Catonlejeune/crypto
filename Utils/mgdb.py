import pymongo
import pandas as pd
from tqdm import tqdm


def push_pandas_mongodb(df, table, conn=None):
    if conn is None:
        conn = pymongo.MongoClient(
            'mongodb+srv://HorseUser:4m8J5Xg53t2gBMW@cluster0.dsgpz.mongodb.net/myFirstDatabase?retryWrites=true&w'
            '=majority')
    db = conn.client['crypto_database']
    collection = db[table]
    df_dict = df.to_dict("records")
    try:
        collection.replace_one({'Open_time': df_dict['Open_time'],
                                'Spot': df_dict['Spot'],
                                'Interval': df_dict['Interval']},
                               df_dict, upsert=True)
    except Exception as e:
        print(e)


def read_mongo(table, ssjacent, interval='1H', conn=None):
    """ Read from Mongo and Store into DataFrame """
    if conn == None:
        conn = pymongo.MongoClient(
            'mongodb+srv://HorseUser:4m8J5Xg53t2gBMW@cluster0.dsgpz.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
    db = conn.client['crypto_database']
    collection = db[table]
    return pd.DataFrame(list(collection.find({'Ssjacent': ssjacent,
                                              'Interval': interval}))).drop(['_id'], axis=1)
