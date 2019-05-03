
import pymongo
from QUANTTOOLS.QAStockETL.QAUtil import ASCENDING
from QUANTAXIS.QAUtil import (DATABASE, QA_util_getBetweenQuarter, QA_util_log_info, QA_util_add_months,
                              QA_util_to_json_from_pandas, QA_util_today_str,
                              QA_util_datetime_to_strdate)
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_indicator
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_list_adv
import pandas as pd

def QA_SU_save_stock_technical_index_day(client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    END_DATE = QA_util_today_str()
    START_DATE = QA_util_datetime_to_strdate(QA_util_add_months(QA_util_today_str(),-24))
    codes = list(QA_fetch_stock_list_adv()['code'])

    stock_technical_index = client.stock_technical_index
    stock_technical_index.create_index([("code", pymongo.ASCENDING),("date", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code, stock_technical_index):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_index==== {}'.format(str(code)), ui_log)

            stock_technical_index.insert_many(QA_util_to_json_from_pandas(
                QA_fetch_get_indicator(code, START_DATE, END_DATE)), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in codes:

        QA_util_log_info('The {} of Total {}'.format
                         ((codes.index(item) +1), len(codes)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(item) +1) / len(codes) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((codes.index(item) +1) / len(codes) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

        __saving_work( item, stock_technical_index)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock_technical_index ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_stock_technical_index_his(client=DATABASE, ui_log = None, start_date=None,end_date=None):

    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    END_DATE = QA_util_today_str()
    START_DATE = "2007-01-01"
    codes = list(QA_fetch_stock_list_adv()['code'])

    stock_technical_index = client.stock_technical_index
    stock_technical_index.create_index([("code", pymongo.ASCENDING),("date", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code, stock_technical_index):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_index==== {}'.format(str(code)), ui_log)

            stock_technical_index.insert_many(QA_util_to_json_from_pandas(
                QA_fetch_get_indicator(code, START_DATE, END_DATE)), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in codes:

        QA_util_log_info('The {} of Total {}'.format
                         ((codes.index(item) +1), len(codes)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(item) +1) / len(codes) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((codes.index(item) +1) / len(codes) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

        __saving_work( item, stock_technical_index)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock_technical_index ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)