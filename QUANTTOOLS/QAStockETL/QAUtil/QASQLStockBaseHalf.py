import cx_Oracle
import pandas as pd
import datetime
from QUANTAXIS.QAUtil import QA_util_log_info
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

sql_text = '''select to_char(order_date, 'yyyy-mm-dd') as "date",
       a.code as "code",
       round(high_qfq/low_qfq - 1,4) as RNG_HALF,
       to_number(RNG_L) as RNG_L_HALF,
       to_number(RNG_5) as RNG_5_HALF,
       to_number(RNG_10) as RNG_10_HALF,
       to_number(RNG_20) as RNG_20_HALF,
       to_number(RNG_30) as RNG_30_HALF,
       to_number(RNG_60) as RNG_60_HALF,
       to_number(RNG_90) as RNG_90_HALF,
       round((case
               when LAG_MARKET = 0 or LAG_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG_MARKET - 1
             end) * 100,
             2) as lag_HALF,
       round((case
               when LAG2_MARKET = 0 or LAG2_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG2_MARKET - 1
             end) * 100,
             2) as lag2_HALF,
       round((case
               when LAG3_MARKET = 0 or LAG3_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG3_MARKET - 1
             end) * 100,
             2) as lag3_HALF,
       round((case
               when LAG5_MARKET = 0 or LAG5_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG5_MARKET - 1
             end) * 100,
             2) as lag5_HALF,
       round((case
               when LAG10_MARKET = 0 or LAG10_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG10_MARKET - 1
             end) * 100,
             2) as lag10_HALF,
       round((case
               when LAG20_MARKET = 0 or LAG20_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG20_MARKET - 1
             end) * 100,
             2) as lag20_HALF,
       round((case
               when LAG30_MARKET = 0 or LAG30_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG30_MARKET - 1
             end) * 100,
             2) as lag30_HALF,
       round((case
               when LAG60_MARKET = 0 or LAG60_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG60_MARKET - 1
             end) * 100,
             2) as lag60_HALF,
       round((case
               when LAG90_MARKET = 0 or LAG90_MARKET is null then
                0
               else
                CLOSE_QFQ / LAG90_MARKET - 1
             end) * 100,
             2) as lag90_HALF,
       round((case
               when AVG5_A_MARKET = 0 or AVG5_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG5_A_MARKET - 1
             end) * 100,
             2) as avg5_HALF,
       round((case
               when AVG10_A_MARKET = 0 or AVG10_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG10_A_MARKET - 1
             end) * 100,
             2) as avg10_HALF,
       round((case
               when AVG20_A_MARKET = 0 or AVG20_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG20_A_MARKET - 1
             end) * 100,
             2) as avg20_HALF,
       round((case
               when AVG30_A_MARKET = 0 or AVG30_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG30_A_MARKET - 1
             end) * 100,
             2) as avg30_HALF,
       round((case
               when AVG60_A_MARKET = 0 or AVG60_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG60_A_MARKET - 1
             end) * 100,
             2) as avg60_HALF,
       round((case
               when AVG90_A_MARKET = 0 or AVG90_A_MARKET is null then
                0
               else
                CLOSE_QFQ / AVG90_A_MARKET - 1
             end) * 100,
             2) as avg90_HALF,
       AMT_L as AMT_L_HALF,
        AMT_5 as AMT_5_HALF,
        AMT_10 as AMT_10_HALF,
        AMT_20 as AMT_20_HALF,
        AMT_30 as AMT_30_HALF,
        AMT_60 as AMT_60_HALF,
        AMT_90 as AMT_90_HALF,
        MAMT_5 as MAMT_5_HALF,
        MAMT_10 as MAMT_10_HALF,
        MAMT_20 as MAMT_20_HALF,
        MAMT_30 as MAMT_30_HALF,
        MAMT_60 as MAMT_60_HALF,
        MAMT_90 as MAMT_90_HALF,
       to_number(avg5_c_market) as avg5_c_HALF,
       to_number(avg10_c_market) as avg10_c_HALF,
       to_number(avg20_c_market) as avg20_c_HALF,
       to_number(avg30_c_market) as avg30_c_HALF,
       to_number(avg60_c_market) as avg60_c_HALF,
       to_number(avg90_c_market) as avg90_c_HALF
from stock_market_half a
where order_Date >=
to_date('{from_}', 'yyyy-mm-dd')
and order_Date <=
to_date('{to_}', 'yyyy-mm-dd')
'''

def QA_Sql_Stock_BaseHalf(from_ , to_, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch Stock QuantData BaseHalf ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)
    sql_text = sql_text.format(from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    return(data.drop_duplicates((['code', 'date'])).set_index(['date','code']))