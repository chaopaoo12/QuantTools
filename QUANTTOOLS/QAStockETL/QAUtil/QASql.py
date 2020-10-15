# coding:utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2018 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import pymongo
import pandas as pd
from motor.motor_asyncio import AsyncIOMotorClient
from sqlalchemy import types, create_engine
import asyncio
import cx_Oracle
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH1 = 'oracle+cx_oracle://{user}:{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)
ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)
mongo_path = 'mongodb://{server}:27017/{database}'.format(database = MongoDB_Database, server = MongoDB_Server)

def QA_util_sql_mongo_setting(uri=mongo_path):
    # 采用@几何的建议,使用uri代替ip,port的连接方式
    # 这样可以对mongodb进行加密:
    # uri=mongodb://user:passwor@ip:port
    client = pymongo.MongoClient(uri)
    return client

# async


def QA_util_sql_async_mongo_setting(uri=mongo_path):
    """异步mongo示例

    Keyword Arguments:
        uri {str} -- [description] (default: {'mongodb://192.168.3.56:27017/quantaxis'})

    Returns:
        [type] -- [description]
    """
    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # async def client():
    return AsyncIOMotorClient(uri, io_loop=loop)
    # yield  client()


def get_connect():
    conn = create_engine(
        ORACLE_PATH1, echo=False)
    # engine 是 from sqlalchemy import create_engine
    connection = conn.raw_connection()
    cursor = connection.cursor()
    # null value become ''
    return cursor


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def QA_util_sql_store_mysql(data, table_name, ORACLE_PATH=ORACLE_PATH1, if_exists="fail"):
    engine = create_engine(ORACLE_PATH)
    columns = list(data.columns)
    for i in range(len(columns)):
        if columns[i].isdigit():
            columns[i] = "column_%s" % (columns[i]).strip(" ").replace(' ', '').upper()
        elif columns[i] == 'INDEX' or columns[i] == 'index':
            columns[i] ='INDEX_COL'
        else:
            if columns[i] == 'date' or columns[i] == 'DATE':
                columns[i] = 'order_date'
            columns[i] = columns[i].strip(" ").replace(' ', '').upper()
    columns = ",".join(columns).replace(
        '-', '_').replace('/', '_').replace(';', '')
    data.columns = columns.split(",")
    columns = ",".join(data.columns)
    dtyp = {c:types.VARCHAR(126)
            for c in data.columns[data.dtypes == 'object'].tolist()}
    try:
        data[:0].to_sql(table_name, engine,
                        if_exists=if_exists, dtype=dtyp)
    except Exception as e:
        QA_util_log_info("Table '%s' already exists." % (table_name))
        print(e)

    #sql_start = "insert into {} ({}) values(%s,%s,%s)".format(table_name, columns)
    sql_end = ",".join([":" + str(i) for i in list(range(1,data.shape[1]+1))])
    sql = "insert into {table_name} ({columns}) values({sql_end})".format(table_name=table_name, columns=columns, sql_end=sql_end)

    conn = cx_Oracle.connect(ORACLE_PATH2)
    cursor = conn.cursor()

    if data.shape[1] > 30:
        break_num = 100000
    else:
        break_num = 1000000
    try:
        for i in chunks([tuple(x) for x in data.where((pd.notnull(data)), None).values], break_num):
            cursor.executemany(sql, i)
        QA_util_log_info("{} has been stored into Table {} Mysql DataBase ".format(
            table_name, table_name))
    except Exception as e:
        conn.rollback()
        QA_util_log_info("执行MySQL: %s 时出错：%s" % (sql, e))
    finally:
        cursor.close()
        conn.commit()
        conn.close()

ASCENDING = pymongo.ASCENDING
DESCENDING = pymongo.DESCENDING
QA_util_sql_mongo_sort_ASCENDING = pymongo.ASCENDING
QA_util_sql_mongo_sort_DESCENDING = pymongo.DESCENDING

if __name__ == '__main__':
    # test async_mongo
    client = QA_util_sql_async_mongo_setting().quantaxis.stock_day
    print(client)
