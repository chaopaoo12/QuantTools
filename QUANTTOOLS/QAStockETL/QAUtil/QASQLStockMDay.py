import cx_Oracle
import pandas as pd
import datetime
from QUANTAXIS.QAUtil import QA_util_log_info
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

sql_text = '''select to_char(ORDER_DATE, 'yyyy-mm-dd') as "date",
CODE AS "code",
open,
       high,
       low,
       close,
       volume,
       amount,
       open_qfq,
       high_qfq,
       low_qfq,
       close_qfq,
       AVG_TOTAL_MARKET,
       LAG_MARKET,
       AVG_LAG_MARKET,
       LAG_HIGH,
       LAG_LOW,
       LAG_AMOUNT,
       LAG2_MARKET,
       AVG_LAG2_MARKET,
       LAG3_MARKET,
       AVG_LAG3_MARKET,
       LAG5_MARKET,
       AVG_LAG5_MARKET,
       LAG10_MARKET,
       AVG_LAG10_MARKET,
       LAG20_MARKET,
       AVG_LAG20_MARKET,
       LAG30_MARKET,
       AVG_LAG30_MARKET,
       LAG30_HIGH,
       LAG30_LOW,
       LAG60_MARKET,
       AVG_LAG60_MARKET,
       LAG60_HIGH,
       LAG60_LOW,
       LAG90_MARKET,
       AVG_LAG90_MARKET,
       LAG90_HIGH,
       LAG90_LOW,
       AVG10_T_MARKET,
       AVG10_A_MARKET,
       HIGH_10,
       LOW_10,
       AVG20_T_MARKET,
       AVG20_A_MARKET,
       HIGH_20,
       LOW_20,
       AVG30_T_MARKET,
       AVG30_A_MARKET,
       HIGH_30,
       LOW_30,
       AVG60_T_MARKET,
       AVG60_A_MARKET,
       HIGH_60,
       LOW_60,
       AVG90_T_MARKET,
       AVG90_A_MARKET,
       HIGH_90,
       LOW_90,
       AVG5_T_MARKET,
       AVG5_A_MARKET,
       HIGH_5,
       LOW_5,
       AVG5_C_MARKET,
       AVG10_C_MARKET,
       AVG20_C_MARKET,
       AVG30_C_MARKET,
       AVG60_C_MARKET,
       AVG90_C_MARKET,
       RNG_L,
       RNG_5,
       RNG_10,
       RNG_20,
       RNG_30,
       RNG_60,
       RNG_90,
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
       NEGRT_CNT5,
       POSRT_CNT5,
       NEGRT_MEAN5,
       POSRT_MEAN5,
       NEGRT_CNT10,
       POSRT_CNT10,
       NEGRT_MEAN10,
       POSRT_MEAN10,
       NEGRT_CNT20,
       POSRT_CNT20,
       NEGRT_MEAN20,
       POSRT_MEAN20,
       NEGRT_CNT30,
       POSRT_CNT30,
       NEGRT_MEAN30,
       POSRT_MEAN30,
       NEGRT_CNT60,
       POSRT_CNT60,
       NEGRT_MEAN60,
       POSRT_MEAN60,
       NEGRT_CNT90,
       POSRT_CNT90,
       NEGRT_MEAN90,
       POSRT_MEAN90
from stock_market_day
where {condition} order_Date >=
to_date('{from_}', 'yyyy-mm-dd')
and order_Date <=
to_date('{to_}', 'yyyy-mm-dd')
'''

def QA_Sql_Stock_MDay(from_ , to_, code=None, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch Stock Market Day ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)

    if code is None or len(code) > 20:
        code_condition = ''
    elif len(code) == 1:
        code_condition = ' code = ' + ','.join(code) + ' and '
    else:
        code_condition = ' code in (' + ','.join(code) + ') and '

    sql_text = sql_text.format(condition = code_condition,from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    return(data.drop_duplicates((['code', 'date'])).set_index(['date','code']))