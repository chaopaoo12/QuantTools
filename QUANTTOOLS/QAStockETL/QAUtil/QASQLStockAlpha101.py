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
ALPHA001,
ALPHA002,
ALPHA003,
ALPHA004,
ALPHA005,
ALPHA006,
ALPHA007,
ALPHA008,
ALPHA009,
ALPHA010,
ALPHA011,
ALPHA012,
ALPHA013,
ALPHA014,
ALPHA015,
ALPHA016,
ALPHA017,
ALPHA018,
ALPHA019,
ALPHA020,
ALPHA021,
ALPHA022,
ALPHA023,
ALPHA024,
ALPHA025,
ALPHA026,
ALPHA027,
ALPHA028,
ALPHA029,
ALPHA030,
ALPHA031,
ALPHA032,
ALPHA033,
ALPHA034,
ALPHA035,
ALPHA036,
ALPHA037,
ALPHA038,
ALPHA039,
ALPHA040,
ALPHA041,
ALPHA042,
ALPHA043,
ALPHA044,
ALPHA045,
ALPHA046,
ALPHA047,
ALPHA049,
ALPHA050,
ALPHA051,
ALPHA052,
ALPHA053,
ALPHA054,
ALPHA055,
ALPHA057,
ALPHA060,
ALPHA061,
ALPHA062,
ALPHA064,
ALPHA065,
ALPHA066,
ALPHA068,
ALPHA071,
ALPHA072,
ALPHA073,
ALPHA074,
ALPHA075,
ALPHA077,
ALPHA078,
ALPHA081,
ALPHA083,
ALPHA085,
ALPHA086,
ALPHA088,
ALPHA092,
ALPHA094,
ALPHA095,
ALPHA096,
ALPHA098,
ALPHA099,
ALPHA101
from stock_alpha101
where order_Date >=
to_date('{from_}', 'yyyy-mm-dd')
and order_Date <=
to_date('{to_}', 'yyyy-mm-dd')
'''

def QA_Sql_Stock_Alpha101(from_ , to_, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch Stock QuantData Alpha101 ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)
    sql_text = sql_text.format(from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    return(data.drop_duplicates((['code', 'date'])).groupby('code').fillna(method='ffill'))