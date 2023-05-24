import cx_Oracle
import pandas as pd
import numpy as np
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

sql_text = '''select code as index_code, stock as code, cate, index_name from index_stock;
'''

def QASQLIndexToStock(code=None, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Fetch Stock To Index', ui_log)

    if code is None or len(code) > 20:
        code_condition = ''
    elif len(code) == 1:
        code_condition = ' stock = ' + ','.join(code)
    else:
        code_condition = ' stock in (' + ','.join(code) + ')'

    sql_text = sql_text.format(condition = code_condition)
    conn = cx_Oracle.connect(ORACLE_PATH2)
    data = pd.read_sql(sql=sql_text, con=conn)
    conn.close()
    data = data.drop_duplicates((['code', 'index_code']))
    return(data)