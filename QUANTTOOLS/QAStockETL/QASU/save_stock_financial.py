
import pymongo
from QUANTTOOLS.QAStockETL.QAUtil import QA_util_etl_stock_quant
from QUANTTOOLS.QAStockETL.QAUtil import ASCENDING
from QUANTAXIS.QAUtil import (DATABASE, QA_util_to_json_from_pandas, QA_util_today_str,QA_util_log_info,
                              QA_util_get_trade_range)
import pandas as pd

def QA_SU_save_stock_fianacial_momgo(start_date=None,end_date=None, ui_log = None, ui_progress = None):
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
                QA_util_log_info('end_date should large than start_date start {_from} end {_to} '.format(_from=start_date, _to=end_date), ui_log)
    col = DATABASE.stock_financial_analysis
    col.create_index(
        [("CODE", ASCENDING), ("date_stamp", ASCENDING)], unique=True)

    deal_date_list = QA_util_get_trade_range(start_date, end_date)
    if deal_date_list is None:
        QA_util_log_info('##JOB Nono Trading Day ============== from {_from} to {_to} '.format(_from=start_date, _to=end_date), ui_log)
    else:
        for deal_date in deal_date_list:
            data = QA_util_etl_stock_quant(deal_date)
            if data is not None:
                data = data.drop_duplicates(
                    (['CODE', 'date']))
                QA_util_log_info(
                    '##JOB01 Pre Data stock financial data ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                data = QA_util_to_json_from_pandas(data)
                QA_util_log_info("got stock financial data ============== {deal_date}".format(deal_date=deal_date), ui_log)
                QA_util_log_info(
                    '##JOB02 Got Data stock financial data ============== {deal_date}'.format(deal_date=deal_date), ui_log)
                try:
                    col.insert_many(data, ordered=False)
                    QA_util_log_info(
                        '##JOB03 Now stock financial data saved ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                except Exception as e:
                    if isinstance(e, MemoryError):
                        col.insert_many(data, ordered=True)
                    elif isinstance(e, pymongo.bulk.BulkWriteError):
                        pass
                pass
            else:
                QA_util_log_info(
                    '##JOB01 No Data stock_fianacial_data ============== {deal_date} '.format(deal_date=deal_date), ui_log)

def QA_SU_save_stock_fianacial_momgo_his(start_date=None,end_date=None, ui_log = None, ui_progress = None):
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
                QA_util_log_info('end_date should large than start_date start {_from} end {_to} '.format(_from=start_date, _to=end_date), ui_log)
    col = DATABASE.stock_financial_analysis
    col.create_index(
        [("CODE", ASCENDING), ("date_stamp", ASCENDING)], unique=True)

    deal_date_list = QA_util_get_trade_range(start_date, end_date)
    if deal_date_list is None:
        QA_util_log_info('##JOB Nono Trading Day ============== from {_from} to {_to} '.format(_from=start_date, _to=end_date), ui_log)
    else:
        for deal_date in deal_date_list:
            data = QA_util_etl_stock_quant(deal_date)
            if data is not None:
                data = data.drop_duplicates(
                    (['CODE', 'date']))
                QA_util_log_info(
                    '##JOB01 Pre Data stock financial data ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                data = QA_util_to_json_from_pandas(data)
                QA_util_log_info("got stock financial data ============== {deal_date}".format(deal_date=deal_date), ui_log)
                QA_util_log_info(
                    '##JOB02 Got Data stock financial data ============== {deal_date}'.format(deal_date=deal_date), ui_log)
                try:
                    col.insert_many(data, ordered=False)
                    QA_util_log_info(
                        '##JOB03 Now stock financial data saved ============== {deal_date} '.format(deal_date=deal_date), ui_log)
                except Exception as e:
                    if isinstance(e, MemoryError):
                        col.insert_many(data, ordered=True)
                    elif isinstance(e, pymongo.bulk.BulkWriteError):
                        pass
                pass
            else:
                QA_util_log_info(
                    '##JOB01 No Data stock_fianacial_data ============== {deal_date} '.format(deal_date=deal_date), ui_log)