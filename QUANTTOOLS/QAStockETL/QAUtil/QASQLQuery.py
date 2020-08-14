import cx_Oracle
import pandas as pd
import datetime
from QUANTAXIS.QAUtil import QA_util_log_info
from  QUANTAXIS.QAUtil import (QA_util_date_stamp,QA_util_today_str,
                               QA_util_if_trade,QA_util_get_pre_trade_date)
from QUANTTOOLS.QAStockETL.QAData.database_settings import (Oracle_Database, Oracle_User, Oracle_Password, Oralce_Server, MongoDB_Server, MongoDB_Database)

ORACLE_PATH2 = '{user}/{password}@{server}:1521/{database}'.format(database = Oracle_Database, password = Oracle_Password, server = Oralce_Server, user = Oracle_User)

sql_text = ''

def QA_util_etl_stock_quant(deal_date = None, sql_text = sql_text, ui_log= None):
    QA_util_log_info(
        '##JOB01 Now Etl Stock QuantData ==== {}'.format(deal_date), ui_log)

    if deal_date is None:
        QA_util_log_info('Must Have A DATE')
    else:
        if QA_util_if_trade(deal_date) == True:
            sql_text = sql_text.format(start_date=deal_date)
            conn = cx_Oracle.connect(ORACLE_PATH2)
            data = pd.read_sql(sql=sql_text, con=conn)
            conn.close()
        else:
            data = None
        if data is None:
            QA_util_log_info("No data For {start_date}".format(start_date=deal_date))
            return None
        else:
            data = data.assign(NETCASHOPERATINRATE_AVG3 = (data.NETCASHOPERATINRATE_LY + data.NETCASHOPERATINRATE_L2Y + data.NETCASHOPERATINRATE_L3Y)/3)
            data = data.assign(NETPRTAX_RATE = data.NETPROFIT_INRATE / data.TOTALPROFITINRATE)
            data = data.assign(OPINRATE_AVG3 = (data.OPERATINGRINRATE_LY + data.OPERATINGRINRATE_L2Y + data.OPERATINGRINRATE_L3Y)/3)
            data = data.assign(NETPINRATE_AVG3 = (data.NETPROFIT_INRATE_LY + data.NETPROFIT_INRATE_L2Y + data.NETPROFIT_INRATE_L3Y)/3)
            data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
            return(data.drop_duplicates((['CODE', 'date_stamp'])))