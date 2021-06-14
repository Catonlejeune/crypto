import pymongo
import dns


def push_pandas_mongodb(df, table, conn=None):
    if conn == None:
        conn = pymongo.MongoClient(
            'mongodb+srv://HorseUser:4m8J5Xg53t2gBMW@cluster0.dsgpz.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
    db = conn.client['crypto_database']
    collection = db[table]
    df_dict = df.to_dict("records")
    try:
        for row in df_dict:
            collection.replace_one({'Open_time': row.get('Open_time'), 'Spot': row.get('Spot')}, row, upsert=True)
    except Exception as e:
        print(e)