import cx_Oracle
import pandas as pd
import datetime
from QUANTAXIS.QAUtil import QA_util_log_info
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

sql_text = '''select to_char(order_date, 'yyyy-mm-dd') as "date",
       a.code,
       round(high_qfq/low_qfq - 1,4) as RNG,
       to_number(RNG_L) as RNG_L,
       to_number(RNG_5) as RNG_5,
       to_number(RNG_10) as RNG_10,
       to_number(RNG_20) as RNG_20,
       to_number(RNG_30) as RNG_30,
       to_number(RNG_60) as RNG_60,
       to_number(RNG_90) as RNG_90,
       round((case
               when LAG_MARKET = 0 or LAG_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG_MARKET - 1
             end) * 100,
             2) as lag,
       round((case
               when LAG2_MARKET = 0 or LAG2_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG2_MARKET - 1
             end) * 100,
             2) as lag2,
       round((case
               when LAG3_MARKET = 0 or LAG3_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG3_MARKET - 1
             end) * 100,
             2) as lag3,
       round((case
               when LAG5_MARKET = 0 or LAG5_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG5_MARKET - 1
             end) * 100,
             2) as lag5,
       round((case
               when LAG10_MARKET = 0 or LAG10_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG10_MARKET - 1
             end) * 100,
             2) as lag10,
       round((case
               when LAG20_MARKET = 0 or LAG20_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG20_MARKET - 1
             end) * 100,
             2) as lag20,
       round((case
               when LAG30_MARKET = 0 or LAG30_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG30_MARKET - 1
             end) * 100,
             2) as lag30,
       round((case
               when LAG60_MARKET = 0 or LAG60_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG60_MARKET - 1
             end) * 100,
             2) as lag60,
       round((case
               when LAG90_MARKET = 0 or LAG90_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG90_MARKET - 1
             end) * 100,
             2) as lag90,
       round((case
               when AVG5_A_MARKET = 0 or AVG5_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG5_A_MARKET - 1
             end) * 100,
             2) as avg5,
       round((case
               when AVG10_A_MARKET = 0 or AVG10_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG10_A_MARKET - 1
             end) * 100,
             2) as avg10,
       round((case
               when AVG20_A_MARKET = 0 or AVG20_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG20_A_MARKET - 1
             end) * 100,
             2) as avg20,
       round((case
               when AVG30_A_MARKET = 0 or AVG30_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG30_A_MARKET - 1
             end) * 100,
             2) as avg30,
       round((case
               when AVG60_A_MARKET = 0 or AVG60_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG60_A_MARKET - 1
             end) * 100,
             2) as avg60,
       round((case
               when AVG90_A_MARKET = 0 or AVG90_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG90_A_MARKET - 1
             end) * 100,
             2) as avg90,
       AMT_L,
       AMT_5,
       AMT_10,
       AMT_20,
       AMT_30,
       AMT_60,
       AMT_90,
       MAMT_5,
       MAMT_10,
       MAMT_20,
       MAMT_30,
       MAMT_60,
       MAMT_90,
       to_number(avg5_c_market) as avg5_c_market,
       to_number(avg10_c_market) as avg10_c_market,
       to_number(avg20_c_market) as avg20_c_market,
       to_number(avg30_c_market) as avg30_c_market,
       to_number(avg60_c_market) as avg60_c_market,
       to_number(avg90_c_market) as avg90_c_market
from usstock_market_day
where order_Date >=
to_date('{from_}', 'yyyy-mm-dd')
and order_Date <=
to_date('{to_}', 'yyyy-mm-dd')
'''

def QA_Sql_USStock_Base(from_ , to_, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch USStock QuantData Base ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)
    sql_text = sql_text.format(from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    return(data.drop_duplicates((['code', 'date'])).set_index(['date','code']).groupby('code').fillna(method='ffill'))