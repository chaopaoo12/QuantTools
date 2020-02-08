from QUANTAXIS.QAUtil import (DATABASE, QA_util_log_info,QA_util_to_json_from_pandas,QA_util_today_str,QA_util_get_trade_range, QA_util_if_trade,QA_util_code_tolist)
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_list_adv,QA_fetch_index_list_adv
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_alpha
import pymongo
import gc

def QA_SU_save_stock_alpha_day(client=DATABASE, ui_log = None, ui_progress = None, code = None, date = None):
    '''
     save stock_day
    保存财报日历
    历史全部数据
    :return:
    '''
    if date is None:
        date = [QA_util_today_str()]
    elif isinstance(date,str):
        date = list([date])
    if code is None:
        code = list(QA_fetch_stock_list_adv()['code'])
    stock_alpha = client.stock_alpha
    stock_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(date, code, stock_alpha):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Stock Alpha==== {}'.format(str(date)), ui_log)
            stock_alpha.insert_many(QA_util_to_json_from_pandas(
                QA_fetch_get_stock_alpha(code, date)), ordered=False)
            gc.collect()
        except Exception as error0:
            print(error0)
            err.append(str(date))

    for item in date:
        QA_util_log_info('The {} of Total {}'.format
                         ((date.index(item) +1), len(date)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((date.index(item) +1) / len(date) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((date.index(item) +1) / len(date) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)
        if QA_util_if_trade(item) == True:
            __saving_work( item, code, stock_alpha)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save Stock Alpha ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_stock_alpha_his(client=DATABASE, ui_log = None, ui_progress = None, code = None, start_date = None, end_date = None):
    '''
    save stock_day
    保存财报日历
    反向查询四个季度财报
    :return:
    '''
    if code is None:
        code = list(QA_fetch_stock_list_adv()['code'])
    if start_date is None:
        start_date = '2005-01-01'
    if end_date is None:
        end_date  = QA_util_today_str()

    deal_date_list = QA_util_get_trade_range(start_date, end_date)
    stock_alpha = client.stock_alpha
    stock_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(date, code, stock_alpha):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Stock Alpha==== {}'.format(str(date)), ui_log)
            stock_alpha.insert_many(QA_util_to_json_from_pandas(
                QA_fetch_get_stock_alpha(code, date)), ordered=False)
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
            __saving_work( item, code, stock_alpha)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save Stock Alpha ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_index_alpha_day(client=DATABASE, ui_log = None, ui_progress = None, code = None, date = None):
    '''
     save index_day
    :return:
    '''
    if date is None:
        date = [QA_util_today_str()]
    elif isinstance(date,str):
        date = list([date])
    if code is None:
        code = list(QA_fetch_index_list_adv()['code'])
    index_alpha = client.index_alpha
    index_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(date, code, index_alpha):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Stock Alpha==== {}'.format(str(date)), ui_log)
            index_alpha.insert_many(QA_util_to_json_from_pandas(
                QA_fetch_get_stock_alpha(code, date)), ordered=False)
            gc.collect()
        except Exception as error0:
            print(error0)
            err.append(str(date))

    for item in date:
        QA_util_log_info('The {} of Total {}'.format
                         ((date.index(item) +1), len(date)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((date.index(item) +1) / len(date) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((date.index(item) +1) / len(date) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)
        if QA_util_if_trade(item) == True:
            __saving_work( item, code, index_alpha)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save Stock Alpha ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_index_alpha_his(client=DATABASE, ui_log = None, ui_progress = None, code = None, start_date = None, end_date = None):
    '''
    save stock_day
    保存财报日历
    反向查询四个季度财报
    :return:
    '''
    if code is None:
        code = list(QA_fetch_index_list_adv()['code'])
    if start_date is None:
        start_date = '2005-01-01'
    if end_date is None:
        end_date  = QA_util_today_str()

    deal_date_list = QA_util_get_trade_range(start_date, end_date)
    index_alpha = client.index_alpha
    index_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(date, code, index_alpha):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Stock Alpha==== {}'.format(str(date)), ui_log)
            index_alpha.insert_many(QA_util_to_json_from_pandas(
                QA_fetch_get_stock_alpha(code, date)), ordered=False)
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
            __saving_work( item, code, index_alpha)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save Stock Alpha ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)