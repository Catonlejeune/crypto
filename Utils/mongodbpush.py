import pymongo
import dns

def push_pandas_mongodb(df,code):
    db = pymongo.MongoClient.client['crypto_database']
    collection = db[code]
    df.reset_index(inplace=True)
    df = df.to_dict("records")
    collection.insert_many(df)