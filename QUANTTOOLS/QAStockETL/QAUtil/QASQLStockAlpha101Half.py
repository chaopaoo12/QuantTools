import cx_Oracle
import pandas as pd
from QUANTAXIS.QAUtil import QA_util_log_info

from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

sql_text = '''select to_char(ORDER_DATE, 'yyyy-mm-dd') as "date",
       CODE AS "code",
       alpha001 as alpha001_half,
       alpha002 as alpha002_half,
       alpha003 as alpha003_half,
       alpha004 as alpha004_half,
       alpha005 as alpha005_half,
       alpha006 as alpha006_half,
       alpha007 as alpha007_half,
       alpha008 as alpha008_half,
       alpha009 as alpha009_half,
       alpha010 as alpha010_half,
       alpha011 as alpha011_half,
       alpha012 as alpha012_half,
       alpha013 as alpha013_half,
       alpha014 as alpha014_half,
       alpha015 as alpha015_half,
       alpha016 as alpha016_half,
       alpha017 as alpha017_half,
       alpha018 as alpha018_half,
       alpha019 as alpha019_half,
       alpha020 as alpha020_half,
       alpha021 as alpha021_half,
       alpha022 as alpha022_half,
       alpha023 as alpha023_half,
       alpha024 as alpha024_half,
       alpha025 as alpha025_half,
       alpha026 as alpha026_half,
       alpha027 as alpha027_half,
       alpha028 as alpha028_half,
       alpha029 as alpha029_half,
       alpha030 as alpha030_half,
       alpha031 as alpha031_half,
       alpha032 as alpha032_half,
       alpha033 as alpha033_half,
       alpha034 as alpha034_half,
       alpha035 as alpha035_half,
       alpha036 as alpha036_half,
       alpha037 as alpha037_half,
       alpha038 as alpha038_half,
       alpha039 as alpha039_half,
       alpha040 as alpha040_half,
       alpha041 as alpha041_half,
       alpha042 as alpha042_half,
       alpha043 as alpha043_half,
       alpha044 as alpha044_half,
       alpha045 as alpha045_half,
       alpha046 as alpha046_half,
       alpha047 as alpha047_half,
       alpha049 as alpha049_half,
       alpha050 as alpha050_half,
       alpha051 as alpha051_half,
       alpha052 as alpha052_half,
       alpha053 as alpha053_half,
       alpha054 as alpha054_half,
       alpha055 as alpha055_half,
       alpha057 as alpha057_half,
       alpha060 as alpha060_half,
       alpha061 as alpha061_half,
       alpha062 as alpha062_half,
       alpha064 as alpha064_half,
       alpha065 as alpha065_half,
       alpha066 as alpha066_half,
       alpha068 as alpha068_half,
       alpha071 as alpha071_half,
       alpha072 as alpha072_half,
       alpha073 as alpha073_half,
       alpha074 as alpha074_half,
       alpha075 as alpha075_half,
       alpha077 as alpha077_half,
       alpha078 as alpha078_half,
       alpha081 as alpha081_half,
       alpha083 as alpha083_half,
       alpha085 as alpha085_half,
       alpha086 as alpha086_half,
       alpha088 as alpha088_half,
       alpha092 as alpha092_half,
       alpha094 as alpha094_half,
       alpha095 as alpha095_half,
       alpha096 as alpha096_half,
       alpha098 as alpha098_half,
       alpha099 as alpha099_half,
       alpha101 as alpha101_half
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