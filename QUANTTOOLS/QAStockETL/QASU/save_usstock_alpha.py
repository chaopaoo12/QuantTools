from QUANTAXIS.QAUtil import (DATABASE, QA_util_log_info,QA_util_to_json_from_pandas,QA_util_today_str)
from QUANTTOOLS.QAStockETL.QAUtil import (QA_util_get_trade_range, QA_util_if_trade)

from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_usstock_list
from QUANTTOOLS.QAStockETL.QAFetch import (QA_fetch_get_usstock_alpha,
                                           QA_fetch_get_usstock_alpha101)
import pymongo
import gc

def QA_SU_save_usstock_alpha_day(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    保存财报日历
    历史全部数据
    :return:
    '''
    if end_date is None:
        end_date = QA_util_today_str()

    if start_date is None:
        start_date = '2009-01-01'

    deal_date_list = QA_util_get_trade_range(start_date, end_date, 'us')

    if code is None:
        code = list(QA_fetch_usstock_list()['code'])

    stock_alpha = client.usstock_alpha
    stock_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(date, code):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving USStock Alpha191==== {}'.format(str(date)), ui_log)
            data = QA_fetch_get_usstock_alpha(code, date)
            if data is not None:
                stock_alpha.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
                gc.collect()
        except Exception as error0:
            print(error0)
            err.append(str(date))

    for item in deal_date_list:
        QA_util_log_info('The {} of Total {}'.format
                         ((deal_date_list.index(item) +1), len(deal_date_list)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((deal_date_list.index(item) +1) / len(deal_date_list) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((deal_date_list.index(item) +1) / len(deal_date_list) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)
        if QA_util_if_trade(item) == True:
            __saving_work( item, code)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save USStock Alpha191 ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_usstock_alpha_his(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
    '''
    save stock_day
    保存财报日历
    反向查询四个季度财报
    :return:
    '''
    if code is None:
        code = list(QA_fetch_usstock_list()['code'])
    if end_date is None:
        end_date = QA_util_today_str()

    if start_date is None:
        start_date = '2009-01-01'

    deal_date_list = QA_util_get_trade_range(start_date, end_date, 'us')

    stock_alpha = client.usstock_alpha
    stock_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code, date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving USStock Alpha191==== {}'.format(str(date)), ui_log)
            data = QA_fetch_get_usstock_alpha(code, date)
            if data is not None:
                stock_alpha.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(date))

    for item in deal_date_list:

        QA_util_log_info('The {} of Total {}'.format
                         ((deal_date_list.index(item) +1), len(deal_date_list)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((deal_date_list.index(item) +1) / len(deal_date_list) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((deal_date_list.index(item) + 1)/ len(deal_date_list) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)
        if QA_util_if_trade(item) == True:
            __saving_work(code, item)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save USStock Alpha191 ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_usstock_alpha101_day(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    保存财报日历
    历史全部数据
    :return:
    '''
    if end_date is None:
        end_date = QA_util_today_str()

    if start_date is None:
        start_date = '2009-01-01'
    codes = code
    if codes is None:
        codes = list(QA_fetch_usstock_list()['code'])

    stock_alpha = client.usstock_alpha101
    stock_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start,end):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving USStock Alpha101==== {}'.format(str(code)), ui_log)
            data = QA_fetch_get_usstock_alpha101(code,start,end)
            if data is not None:
                stock_alpha.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
                gc.collect()
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for code in codes:
        QA_util_log_info('The {} of Total {}'.format
                         ((codes.index(code) +1), len(codes)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(code) +1) / len(codes) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((codes.index(code) +1) / len(codes) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)
        __saving_work(code,start_date,end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save USStock Alpha101 ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_usstock_alpha101_his(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    保存财报日历
    历史全部数据
    :return:
    '''
    if end_date is None:
        end_date = QA_util_today_str()

    if start_date is None:
        start_date = '2009-01-01'
    codes = code
    if codes is None:
        codes = list(QA_fetch_usstock_list()['code'])

    stock_alpha = client.usstock_alpha101
    stock_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start,end):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving USStock Alpha101==== {}'.format(str(code)), ui_log)
            data = QA_fetch_get_usstock_alpha101(code,start,end)
            if data is not None:
                stock_alpha.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
                gc.collect()
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for code in codes:
        QA_util_log_info('The {} of Total {}'.format
                         ((codes.index(code) +1), len(codes)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(code) +1) / len(codes) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((codes.index(code) +1) / len(codes) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)
        __saving_work(code,start_date,end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save USStock Alpha101 ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

if __name__ == '__main__':
    pass