import cx_Oracle
import pandas as pd
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

sql_text = '''select to_char(ORDER_DATE, 'yyyy-mm-dd') as "date",
CODE AS "code",INDUSTRY,ln(TOTAL_MARKET) as TOTAL_MARKET,
case
         when TOTAL_MARKET * TRA_RATE + 1 < 0 then
          null
         else
          TOTAL_MARKET * TRA_RATE / 100 + 1
       end as TRA_RATE, 
case
when total_market * tra_rate / 100000000000 >= 100 then
  0
 when total_market * tra_rate / 100000000000 >= 10 then
  1
 when total_market * tra_rate / 100000000000 >= 5 then
  2
 when total_market * tra_rate / 100000000000 >= 3 then
  3
 when total_market * tra_rate / 100000000000 < 3 then
  4
 else
  5
end as stock_type,
               DAYS,
AVG5,AVG10,AVG20,AVG30,AVG60,
LAG,LAG2,LAG3,LAG5,LAG10,LAG20,LAG30,LAG60,
AVG5_TOR, AVG20_TOR,AVG30_TOR,AVG60_TOR,
GROSSMARGIN,NETPROFIT_INRATE,OPERATINGRINRATE,NETCASHOPERATINRATE,
PB, PBG, PC, PE_TTM, PEEGL_TTM, PEG, PM, PS,PSG,PT,
PE_RATE,PEEGL_RATE,PB_RATE,ROE_RATE,ROE_TTM,ROA_RATE,GROSS_RATE,
RNG,RNG_L,RNG_5,RNG_10,RNG_20, RNG_30, RNG_60, RNG_90,
AMT_L,AMT_5,AMT_10,AMT_20,AMT_30,AMT_60,AMT_90,
MAMT_5,MAMT_10,MAMT_20,MAMT_30,MAMT_60,MAMT_90,
NEG5_RT,NEG5_RATE,NEG10_RT,NEG10_RATE,NEG20_RT,NEG20_RATE,
NEG30_RT,NEG30_RATE,NEG60_RT,NEG60_RATE,NEG90_RT,NEG90_RATE,
AVG5_RNG,AVG10_RNG,AVG20_RNG,AVG30_RNG,AVG60_RNG,ROA, ROE, 
AVG5_CR, AVG10_CR,AVG20_CR,AVG30_CR,AVG60_CR,
AVG5_TR,AVG10_TR,AVG20_TR,AVG30_TR,AVG60_TR,TOTALPROFITINRATE,
AVG10_C_MARKET,AVG20_C_MARKET,AVG60_C_MARKET
from STOCK_QUANT_FINANCIAL
where {condition} order_Date >=
to_date('{from_}', 'yyyy-mm-dd')
and order_Date <=
to_date('{to_}', 'yyyy-mm-dd')
'''

def QA_Sql_Stock_Financial(from_ , to_, code = None, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch Stock QuantData Financial ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)
    if code is None or len(code) > 20:
        code_condition = ''
    else:
        code_condition = ' code in (' + ','.join(code) + ') and '
    sql_text = sql_text.format(condition = code_condition,from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    return(data.drop_duplicates((['code', 'date'])).set_index(['date','code']))