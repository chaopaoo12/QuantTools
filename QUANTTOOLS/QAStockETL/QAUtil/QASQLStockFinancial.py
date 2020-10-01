import cx_Oracle
import pandas as pd
import datetime
from QUANTAXIS.QAUtil import QA_util_log_info
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

sql_text = '''select to_char(ORDER_DATE, 'yyyy-mm-dd') as "date",
CODE AS "code",INDUSTRY,ln(TOTAL_MARKET) as TOTAL_MARKET,
TRA_RATE as TRA_RATE, DAYS,
AVG5,AVG10,AVG20,AVG30,AVG60,
LAG,LAG5,LAG10,LAG20,LAG30,LAG60,
AVG5_TOR, AVG20_TOR,AVG30_TOR,AVG60_TOR,
GROSSMARGIN,NETPROFIT_INRATE,OPERATINGRINRATE,NETCASHOPERATINRATE,
PB, PBG, PC, PE_TTM, PEEGL_TTM, PEG, PM, PS,PSG,PT,
PE_RATE,PEEGL_RATE,PB_RATE,ROE_RATE,ROE_RATET,ROA_RATE,ROA_RATET,
GROSS_RATE,RNG,RNG_L,RNG_5,RNG_10,RNG_20, RNG_30, RNG_60,
AVG5_RNG,AVG10_RNG,AVG20_RNG,AVG30_RNG,AVG60_RNG,ROA, ROE, 
AVG5_CR, AVG10_CR,AVG20_CR,AVG30_CR,AVG60_CR,
AVG5_TR,AVG10_TR,AVG20_TR,AVG30_TR,AVG60_TR,TOTALPROFITINRATE
from STOCK_QUANT_FINANCIAL
where order_Date >=
to_date('{from_}', 'yyyy-mm-dd')
and order_Date <=
to_date('{to_}', 'yyyy-mm-dd')
'''

def QA_Sql_Stock_Financial(from_ , to_, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch Stock QuantData Financial ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)
    sql_text = sql_text.format(from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    return(data.drop_duplicates((['code', 'date'])).set_index(['date','code']).groupby('code').fillna(method='ffill'))