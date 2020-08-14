import cx_Oracle
import pandas as pd
import datetime
from QUANTAXIS.QAUtil import QA_util_log_info
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

sql_text = '''select ORDER_DATE,
CODE,
ALPHA_001,
ALPHA_002,
ALPHA_003,
ALPHA_004,
ALPHA_005,
ALPHA_006,
ALPHA_007,
ALPHA_008,
ALPHA_009,
ALPHA_010,
ALPHA_011,
ALPHA_012,
ALPHA_013,
ALPHA_014,
ALPHA_015,
ALPHA_016,
ALPHA_017,
ALPHA_018,
ALPHA_019,
ALPHA_020,
ALPHA_021,
ALPHA_022,
ALPHA_023,
ALPHA_024,
ALPHA_025,
ALPHA_026,
ALPHA_028,
ALPHA_029,
ALPHA_031,
ALPHA_032,
ALPHA_033,
ALPHA_034,
ALPHA_035,
ALPHA_036,
ALPHA_037,
ALPHA_038,
ALPHA_039,
ALPHA_040,
ALPHA_041,
ALPHA_042,
ALPHA_043,
ALPHA_044,
ALPHA_045,
ALPHA_046,
ALPHA_047,
ALPHA_048,
ALPHA_049,
ALPHA_052,
ALPHA_053,
ALPHA_054,
ALPHA_056,
ALPHA_057,
ALPHA_058,
ALPHA_059,
ALPHA_060,
ALPHA_061,
ALPHA_062,
ALPHA_063,
ALPHA_064,
ALPHA_065,
ALPHA_066,
ALPHA_067,
ALPHA_068,
ALPHA_070,
ALPHA_071,
ALPHA_072,
ALPHA_074,
ALPHA_076,
ALPHA_077,
ALPHA_078,
ALPHA_079,
ALPHA_080,
ALPHA_081,
ALPHA_082,
ALPHA_083,
ALPHA_084,
ALPHA_085,
ALPHA_086,
ALPHA_087,
ALPHA_088,
ALPHA_089,
ALPHA_090,
ALPHA_091
from stock_alpha191
where to_Date(order_Date, 'yyyy-mm-dd') >=
to_date('{from_}', 'yyyy-mm-dd')
and to_Date(order_Date, 'yyyy-mm-dd') <=
to_date('{to_}', 'yyyy-mm-dd')
'''

def QA_Sql_Stock_Alpha191(from_ , to_, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch Stock QuantData Alpha191 ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)
    sql_text = sql_text.format(from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    return(data.drop_duplicates((['CODE', 'ORDER_DATE'])))