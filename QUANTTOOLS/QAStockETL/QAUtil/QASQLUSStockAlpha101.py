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
alpha001,alpha002,alpha003,alpha004,alpha005,alpha006,
alpha007,alpha008,alpha009,alpha010,alpha011,alpha012,
alpha013,alpha014,alpha015,alpha016,alpha017,alpha018,
alpha019,alpha020,alpha021,alpha022,alpha023,alpha024,
alpha025,alpha026,alpha027,alpha028,alpha029,alpha030,
alpha031,alpha032,alpha033,alpha034,alpha035,alpha036,
alpha037,alpha038,alpha039,alpha040,alpha041,alpha042,
alpha043,alpha044,alpha045,alpha046,alpha047,alpha049,
alpha050,alpha051,alpha052,alpha053,alpha054,alpha055,
alpha057,alpha060,alpha061,alpha062,alpha064,alpha065,
alpha066,alpha068,alpha071,alpha072,alpha073,alpha074,
alpha075,alpha077,alpha078,alpha081,alpha083,alpha085,
alpha086,alpha088,alpha092,alpha094,alpha095,alpha096,
alpha098,alpha099,alpha101
from usstock_alpha101
where order_Date >=
to_date('{from_}', 'yyyy-mm-dd')
and order_Date <=
to_date('{to_}', 'yyyy-mm-dd')
'''

def QA_Sql_USStock_Alpha101(from_ , to_, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch USStock QuantData Alpha101 ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)
    sql_text = sql_text.format(from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    return(data.drop_duplicates((['code', 'date'])).set_index(['date','code']).groupby('code').fillna(method='ffill'))