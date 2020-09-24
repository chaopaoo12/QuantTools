from QUANTAXIS.QAUtil import (DATABASE, QA_util_log_info,QA_util_to_json_from_pandas,QA_util_today_str,QA_util_get_trade_range, QA_util_if_trade,QA_util_code_tolist)
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_index_list_adv
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_all,QA_fetch_stock_om_all
from QUANTTOOLS.QAStockETL.QAFetch import (QA_fetch_get_stock_alpha,QA_fetch_get_index_alpha,
                                           QA_fetch_get_stock_alpha101,QA_fetch_get_index_alpha101,
                                           QA_fetch_get_stock_alpha101_half,
                                           QA_fetch_get_stock_alpha191_half)
import pymongo
import gc

def QA_SU_save_stock_alpha_day(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
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

    deal_date_list = QA_util_get_trade_range(start_date, end_date)

    if code is None:
        code = list(QA_fetch_stock_om_all()['code'])

    stock_alpha = client.stock_alpha
    stock_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(date, code):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Stock Alpha191==== {}'.format(str(date)), ui_log)
            data = QA_fetch_get_stock_alpha(code, date)
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
        QA_util_log_info('SUCCESS save Stock Alpha191 ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_stock_alpha_his(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
    '''
    save stock_day
    保存财报日历
    反向查询四个季度财报
    :return:
    '''
    if code is None:
        code = list(QA_fetch_stock_all()['code'])
    if end_date is None:
        end_date = QA_util_today_str()

    if start_date is None:
        start_date = '2009-01-01'

    deal_date_list = QA_util_get_trade_range(start_date, end_date)

    stock_alpha = client.stock_alpha
    stock_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code, date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Stock Alpha191==== {}'.format(str(date)), ui_log)
            data = QA_fetch_get_stock_alpha(code, date)
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
        QA_util_log_info('SUCCESS save Stock Alpha191 ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_index_alpha_day(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save index_day
    :return:
    '''
    if code is None:
        code = list(QA_fetch_index_list_adv()['code'])

    if end_date is None:
        end_date = QA_util_today_str()

    if start_date is None:
        start_date = '2009-01-01'

    deal_date_list = QA_util_get_trade_range(start_date, end_date)

    index_alpha = client.index_alpha
    index_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code, date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Index Alpha191==== {}'.format(date), ui_log)
            data = QA_fetch_get_index_alpha(code, date)
            if data is not None:
                index_alpha.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
            __saving_work(code, item)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save Index Alpha191 ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_index_alpha_his(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
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

    def __saving_work(code, date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Index Alpha191==== {}'.format(str(date)), ui_log)
            data = QA_fetch_get_index_alpha(code, date)
            if data is not None:
                index_alpha.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save Index Alpha191 ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_index_alpha101_day(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save index_day
    :return:
    '''
    codes = code
    if codes is None:
        codes = list(QA_fetch_index_list_adv()['code'])

    if end_date is None:
        end_date = QA_util_today_str()

    if start_date is None:
        start_date = '2009-01-01'

    index_alpha = client.index_alpha101
    index_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code, start, end):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Index Alpha101==== {}'.format(code), ui_log)
            data = QA_fetch_get_index_alpha101(code, start, end)
            if data is not None:
                index_alpha.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save Index Alpha101 ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_alpha101_day(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
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
        codes = list(QA_fetch_stock_om_all()['code'])

    stock_alpha = client.stock_alpha101
    stock_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start,end):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Stock Alpha101==== {}'.format(str(code)), ui_log)
            data = QA_fetch_get_stock_alpha101(code,start,end)
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
        QA_util_log_info('SUCCESS save Stock Alpha101 ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_alpha101_his(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
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
        codes = list(QA_fetch_stock_all()['code'])

    stock_alpha = client.stock_alpha101
    stock_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start,end):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Stock Alpha101==== {}'.format(str(code)), ui_log)
            data = QA_fetch_get_stock_alpha101(code,start,end)
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
        QA_util_log_info('SUCCESS save Stock Alpha101 ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_stock_alpha101half_day(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    保存财报日历
    历史全部数据
    :return:
    '''
    if end_date is None:
        end_date = QA_util_today_str()

    if start_date is None:
        start_date = '2017-04-10'
    codes = code
    if codes is None:
        codes = list(QA_fetch_stock_om_all()['code'])

    stock_alpha = client.stock_alpha101_half
    stock_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start,end):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Stock Alpha101 Half==== {}'.format(str(code)), ui_log)
            data = QA_fetch_get_stock_alpha101_half(code,start,end)
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
        QA_util_log_info('SUCCESS save Stock Alpha101 Half ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_alpha101half_his(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
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
        codes = list(QA_fetch_stock_all()['code'])

    stock_alpha = client.stock_alpha101_half
    stock_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start,end):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Stock Alpha101 Half==== {}'.format(str(code)), ui_log)
            data = QA_fetch_get_stock_alpha101_half(code,start,end)
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
        QA_util_log_info('SUCCESS save Stock Alpha101 Half ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_alpha191half_day(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    保存财报日历
    历史全部数据
    :return:
    '''
    if end_date is None:
        end_date = QA_util_today_str()

    if start_date is None:
        start_date = '2017-04-10'
    codes = code
    if codes is None:
        codes = list(QA_fetch_stock_om_all()['code'])

    stock_alpha = client.stock_alpha191_half
    stock_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start,end):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Stock Alpha191 Half==== {}'.format(str(code)), ui_log)
            data = QA_fetch_get_stock_alpha191_half(code,start,end)
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
        QA_util_log_info('SUCCESS save Stock Alpha191 Half ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_alpha191half_his(code = None, start_date = None, end_date = None, client=DATABASE, ui_log = None, ui_progress = None):
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
        codes = list(QA_fetch_stock_all()['code'])

    stock_alpha = client.stock_alpha191_half
    stock_alpha.create_index([("code", pymongo.ASCENDING), ("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start,end):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving Stock Alpha191 Half==== {}'.format(str(code)), ui_log)
            data = QA_fetch_get_stock_alpha191_half(code,start,end)
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
        QA_util_log_info('SUCCESS save Stock Alpha191 Half ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)