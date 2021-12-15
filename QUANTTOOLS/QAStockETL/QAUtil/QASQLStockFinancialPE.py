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
PE_30VAL,PE_30DN,PE_30UP,PEEGL_30VAL,PEEGL_30DN,PEEGL_30UP,PB_30VAL,PB_30DN,PB_30UP,PS30VAL as PS_30VAL,PS_30DN,PS_30UP,
PE_60VAL,PE_60DN,PE_60UP,PEEGL_60VAL,PEEGL_60DN,PEEGL_60UP,PB_60VAL,PB_60DN,PB_60UP,PS60VAL as PS_60VAL,PS_60DN,PS_60UP,
PE_90VAL,PE_90DN,PE_90UP,PEEGL_90VAL,PEEGL_90DN,PEEGL_90UP,PB_90VAL,PB_90DN,PB_90UP,PS90VAL as PS_90VAL,PS_90DN,PS_90UP
from STOCK_QUANT_FINANCIAL_PERCENT
where {condition} order_Date >=
to_date('{from_}', 'yyyy-mm-dd')
and order_Date <=
to_date('{to_}', 'yyyy-mm-dd')
'''

def QA_Sql_Stock_FinancialPercent(from_ , to_, code =None, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch Stock QuantData Financial Percent ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)

    if code is None or len(code) > 20:
        code_condition = ''
    elif len(code) == 1:
        code_condition = ' code = ' + ','.join(code) + ' and '
    else:
        code_condition = ' code in (' + ','.join(code) + ') and '
    QA_util_log_info(code_condition)
    sql_text = sql_text.format(condition = code_condition,from_=from_,to_=to_)
    QA_util_log_info(sql_text)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    return(data.drop_duplicates((['code', 'date'])).set_index(['date','code']))