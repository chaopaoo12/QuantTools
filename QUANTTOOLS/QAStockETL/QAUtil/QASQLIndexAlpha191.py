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
alpha_001,alpha_002,alpha_003,alpha_004,alpha_005,alpha_006,alpha_007,alpha_008,
alpha_009,alpha_010,alpha_011,alpha_012,alpha_013,alpha_014,alpha_015,alpha_016,
alpha_018,alpha_019,alpha_020,alpha_021,alpha_022,alpha_023,alpha_024,
alpha_025,alpha_026,alpha_028,alpha_029,alpha_031,alpha_032,alpha_033,alpha_034,
alpha_035,alpha_036,alpha_037,alpha_038,alpha_039,alpha_040,alpha_041,alpha_042,
alpha_043,alpha_044,alpha_045,alpha_046,alpha_047,alpha_048,alpha_049,alpha_052,
alpha_053,alpha_054,alpha_056,alpha_057,alpha_058,alpha_059,alpha_060,
alpha_061,alpha_062,alpha_063,alpha_064,alpha_065,alpha_066,alpha_067,alpha_068,
alpha_070,alpha_071,alpha_072,alpha_074,alpha_076,alpha_077,alpha_078,alpha_079,
alpha_080,alpha_081,alpha_082,alpha_083,alpha_084,alpha_085,alpha_086,alpha_087,
alpha_088,alpha_089,alpha_090,alpha_091,alpha_093,alpha_094,alpha_095,
alpha_096,alpha_097,alpha_098,alpha_099,alpha_100,alpha_101,alpha_102,alpha_103,
alpha_104,alpha_105,alpha_106,alpha_107,alpha_108,alpha_109,alpha_111,
alpha_112,alpha_114,alpha_115,alpha_117,alpha_118,alpha_119,
alpha_120,alpha_122,alpha_123,alpha_124,alpha_125,alpha_126,
alpha_129,alpha_130,alpha_132,alpha_133,alpha_134,alpha_135,alpha_136,
alpha_139,alpha_141,alpha_142,alpha_145,
alpha_148,alpha_150,alpha_152,alpha_153,alpha_154,alpha_155,alpha_156,
alpha_157,alpha_158,alpha_159,alpha_160,alpha_161,alpha_162,alpha_163,alpha_164,
alpha_167,alpha_168,alpha_169,alpha_170,alpha_172,alpha_173,alpha_174,
alpha_175,alpha_176,alpha_177,alpha_178,alpha_179,alpha_180,alpha_184,alpha_185,
alpha_186,alpha_187,alpha_188,alpha_189,alpha_191
from index_alpha191
where order_Date >=
to_date('{from_}', 'yyyy-mm-dd')
and order_Date <=
to_date('{to_}', 'yyyy-mm-dd')
'''

def QA_Sql_Index_Alpha191(from_ , to_, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch Index QuantData Alpha191 ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)
    sql_text = sql_text.format(from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    return(data.drop_duplicates((['code', 'date'])).set_index(['date','code']))