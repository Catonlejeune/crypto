import os
import pandas as pd
import pymysql


def get_environnement():
    """
    Get data in the onnection_aws.txt
    :return: dict with connection data
    """
    return pd.read_csv(fr'C:\Users\connection_aws.txt',
                sep="=", header=None).set_index(0)[1].to_dict()


def get_sql_connection():
    """
    Get sql connection
    :return:  connection
    """
    dict_data = get_environnement()
    return pymysql.connect(host=dict_data['ENDPOINT'], user=dict_data['USR'],
                           passwd=dict_data['PASSWORD '], database=dict_data['DBNAME'])


def insert_update_sql(df, table, primary_key, conn=None, do_update=True):
    """
    Insert or update the table
    :param df: Dataframe of the data that we want to push into the DB
    :param table:  The table where we'll push data
    :param primary_key: Primary keys of the table
    :param conn: The conncetion
    """
    if conn is None:
        conn = get_sql_connection()

    query = f"""Insert INTO {table}({' ,'.join(col for col in df.columns)}) VALUES"""
    for idx, row in df.iterrows():
        query += f"""{tuple([values for values in row])},"""
    query = query[:-1] + """ ON DUPLICATE KEY UPDATE """
    if do_update:
        query += f"""{' ,'.join( col + '=values('+col+')' for col in df.columns[~df.columns.isin(primary_key)])};"""
    else:
        query += f"""{' ,'.join( col + '=' +col  for col in df.columns[~df.columns.isin(primary_key)])};"""

    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
