import os
import pandas as pd
import pymysql


def get_environnement():
    """
    Get data in the onnection_aws.txt
    :return: dict with connection data
    """
    return \
    pd.read_csv(fr'{os.path.dirname(os.path.abspath("connection_aws.txt"))}\crypto\lib_crypto\connection_aws.txt',
                sep="=", header=None).set_index(0)[1].to_dict()


def get_sql_connection():
    """
    Get sql connection
    :return:  connection
    """
    dict_data = get_environnement()
    return pymysql.connect(host=dict_data['ENDPOINT'], user=dict_data['USR'],
                           passwd=dict_data['PASSWORD '], database=dict_data['DBNAME'])


def insert_update_sql(df, table, primary_key, conn=None, update=True):
    """
    Insert or update the table
    :param df: Dataframe of the data that we want to push into the DB
    :param table:  The table where we'll push data
    :param primary_key: Primary keys of the table
    :param conn: The conncetion
    """
    if conn is None:
        conn = get_sql_connection()

    query = f"""Insert INTO {table}({tuple([col for col in df.columns])}) VALUES"""
    for idx, row in df.iterrow():
        query += f"""{tuple([values for values in row])},"""
    query = query[:-1] + """ ON DUPLICATE KEY UPDATE"""
    if update:
        for col in df.columns[~df.columns.isin(primary_key)]:
            query += f"""{col + '= values(' + col + '),'}"""
    else:
        for col in df.columns[~df.columns.isin(primary_key)]:
            query += f"""{col + '= ' + col + ','}"""
    query = query[:-1]

    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
