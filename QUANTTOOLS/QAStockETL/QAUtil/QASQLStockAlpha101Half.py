import cx_Oracle
import pandas as pd
from QUANTAXIS.QAUtil import QA_util_log_info

from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

sql_text = '''select to_char(ORDER_DATE, 'yyyy-mm-dd') as "date",
       CODE AS "code",
        ALPHA001 as ALPHA001_HALF,
        ALPHA002 as ALPHA002_HALF,
        ALPHA003 as ALPHA003_HALF,
        ALPHA004 as ALPHA004_HALF,
        ALPHA005 as ALPHA005_HALF,
        ALPHA006 as ALPHA006_HALF,
        ALPHA007 as ALPHA007_HALF,
        ALPHA008 as ALPHA008_HALF,
        ALPHA009 as ALPHA009_HALF,
        ALPHA010 as ALPHA010_HALF,
        ALPHA011 as ALPHA011_HALF,
        ALPHA012 as ALPHA012_HALF,
        ALPHA013 as ALPHA013_HALF,
        ALPHA014 as ALPHA014_HALF,
        ALPHA015 as ALPHA015_HALF,
        ALPHA016 as ALPHA016_HALF,
        ALPHA017 as ALPHA017_HALF,
        ALPHA018 as ALPHA018_HALF,
        ALPHA019 as ALPHA019_HALF,
        ALPHA020 as ALPHA020_HALF,
        ALPHA021 as ALPHA021_HALF,
        ALPHA022 as ALPHA022_HALF,
        ALPHA023 as ALPHA023_HALF,
        ALPHA024 as ALPHA024_HALF,
        ALPHA025 as ALPHA025_HALF,
        ALPHA026 as ALPHA026_HALF,
        ALPHA027 as ALPHA027_HALF,
        ALPHA028 as ALPHA028_HALF,
        ALPHA029 as ALPHA029_HALF,
        ALPHA030 as ALPHA030_HALF,
        ALPHA031 as ALPHA031_HALF,
        ALPHA032 as ALPHA032_HALF,
        ALPHA033 as ALPHA033_HALF,
        ALPHA034 as ALPHA034_HALF,
        ALPHA035 as ALPHA035_HALF,
        ALPHA036 as ALPHA036_HALF,
        ALPHA037 as ALPHA037_HALF,
        ALPHA038 as ALPHA038_HALF,
        ALPHA039 as ALPHA039_HALF,
        ALPHA040 as ALPHA040_HALF,
        ALPHA041 as ALPHA041_HALF,
        ALPHA042 as ALPHA042_HALF,
        ALPHA043 as ALPHA043_HALF,
        ALPHA044 as ALPHA044_HALF,
        ALPHA045 as ALPHA045_HALF,
        ALPHA046 as ALPHA046_HALF,
        ALPHA047 as ALPHA047_HALF,
        ALPHA049 as ALPHA049_HALF,
        ALPHA050 as ALPHA050_HALF,
        ALPHA051 as ALPHA051_HALF,
        ALPHA052 as ALPHA052_HALF,
        ALPHA053 as ALPHA053_HALF,
        ALPHA054 as ALPHA054_HALF,
        ALPHA055 as ALPHA055_HALF,
        ALPHA057 as ALPHA057_HALF,
        ALPHA060 as ALPHA060_HALF,
        ALPHA061 as ALPHA061_HALF,
        ALPHA062 as ALPHA062_HALF,
        ALPHA064 as ALPHA064_HALF,
        ALPHA065 as ALPHA065_HALF,
        ALPHA066 as ALPHA066_HALF,
        ALPHA068 as ALPHA068_HALF,
        ALPHA071 as ALPHA071_HALF,
        ALPHA072 as ALPHA072_HALF,
        ALPHA073 as ALPHA073_HALF,
        ALPHA074 as ALPHA074_HALF,
        ALPHA075 as ALPHA075_HALF,
        ALPHA077 as ALPHA077_HALF,
        ALPHA078 as ALPHA078_HALF,
        ALPHA081 as ALPHA081_HALF,
        ALPHA083 as ALPHA083_HALF,
        ALPHA085 as ALPHA085_HALF,
        ALPHA086 as ALPHA086_HALF,
        ALPHA088 as ALPHA088_HALF,
        ALPHA092 as ALPHA092_HALF,
        ALPHA094 as ALPHA094_HALF,
        ALPHA095 as ALPHA095_HALF,
        ALPHA096 as ALPHA096_HALF,
        ALPHA098 as ALPHA098_HALF,
        ALPHA099 as ALPHA099_HALF,
        ALPHA101 as ALPHA101_HALF
  from stock_alpha101_half
 where order_Date >= to_date('{from_}', 'yyyy-mm-dd')
   and order_Date <= to_date('{to_}', 'yyyy-mm-dd')
'''

def QA_Sql_Stock_Alpha101Half(from_ , to_, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch Stock QuantData Alpha101 Half ==== from {from_} to {to_}'.format(from_=from_,to_=to_), ui_log)
    sql_text = sql_text.format(from_=from_,to_=to_)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    return(data.drop_duplicates((['code', 'date'])).set_index(['date','code']).groupby('code').fillna(method='ffill'))