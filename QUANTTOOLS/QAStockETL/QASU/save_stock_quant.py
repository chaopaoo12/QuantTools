
import pymongo
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_quant_data
from QUANTTOOLS.QAStockETL.QAUtil import ASCENDING
from QUANTAXIS.QAUtil import (DATABASE, QA_util_to_json_from_pandas, QA_util_today_str,QA_util_log_info,
                              QA_util_get_trade_range,QA_util_if_trade)
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_list_adv
import pandas as pd

def QA_SU_save_stock_quant_day(code=None, start_date=None,end_date=None, ui_log = None, ui_progress = None):
    if start_date is None:
        if end_date is None:
            start_date = QA_util_today_str()
            end_date = start_date
        elif end_date is not None:
            start_date = '2008-01-01'
    elif start_date is not None:
        if end_date == None:
            end_date = QA_util_today_str()
        elif end_date is not None:
            if end_date < start_date:
                print('end_date should large than start_date')
    if code is None:
        code = list(QA_fetch_stock_list_adv()['code'])

    col = DATABASE.stock_quant_data
    col.create_index(
        [("code", ASCENDING), ("date_stamp", ASCENDING)], unique=True)
    data1 = QA_fetch_get_quant_data(code, start_date, end_date)
    QA_util_log_info(
        '##JOB got Data stock quant data ============== from {from_} to {to_} '.format(from_=start_date,to_=end_date), ui_log)
    deal_date_list = QA_util_get_trade_range(start_date, end_date)
    if deal_date_list is None:
        print('not a trading day')
    else:
        for deal_date in deal_date_list:
            if QA_util_if_trade(deal_date):
                data = data1[data1['date']==deal_date]
            else:
                data = None
            if data is not None:
                data = data.drop_duplicates(
                    (['code', 'date']))
                QA_util_log_info(
                    '##JOB01 Pre Data stock quant data ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                data = QA_util_to_json_from_pandas(data)
                QA_util_log_info(
                    '##JOB02 Got Data stock quant data ============== {deal_date}'.format(deal_date=deal_date), ui_log)
                try:
                    col.insert_many(data, ordered=False)
                    QA_util_log_info(
                        '##JOB03 Now stock quant data saved ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                except Exception as e:
                    if isinstance(e, MemoryError):
                        col.insert_many(data, ordered=True)
                    elif isinstance(e, pymongo.bulk.BulkWriteError):
                        pass
                pass
            else:
                QA_util_log_info(
                    '##JOB01 No Data stock_quant_datat ============== {deal_date} '.format(deal_date=deal_date), ui_log)
