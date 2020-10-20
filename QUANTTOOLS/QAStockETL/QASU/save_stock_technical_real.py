
import pymongo
from QUANTAXIS.QAUtil import (DATABASE, QA_util_getBetweenQuarter, QA_util_log_info, QA_util_add_months,
                              QA_util_to_json_from_pandas, QA_util_today_str,QA_util_get_pre_trade_date,
                              QA_util_datetime_to_strdate,QA_util_code_tolist)
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_all,QA_fetch_stock_om_all
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_indicator_half,QA_fetch_get_stock_indicator_halfreal

def QA_SU_save_stock_technical_index_half(codes=None, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = QA_util_get_pre_trade_date(QA_util_today_str(),3)
        else:
            start_date = QA_util_get_pre_trade_date(end_date,3)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,3)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = list(QA_fetch_stock_om_all()['code'])
    else:
        codes = QA_util_code_tolist(codes)

    stock_technical_index = client.stock_technical_index_half
    stock_technical_index.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_half from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator_half(code, start_date, end_date,'day')
            if data is not None:
                stock_technical_index.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in codes:

        QA_util_log_info('The {} of Total {}'.format
                         ((codes.index(item) +1), len(codes)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(item) +1) / len(codes) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((codes.index(item) +1) / len(codes) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

        __saving_work( item,start_date,end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock_technical_index half ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_stock_technical_index_half_his(codes=None, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = "2006-01-01"
        else:
            start_date = QA_util_get_pre_trade_date(end_date,3)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,3)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = list(QA_fetch_stock_all()['code'])
    else:
        codes = QA_util_code_tolist(codes)

    stock_technical_index = client.stock_technical_index_half
    stock_technical_index.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_index half from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator_half(code, start_date, end_date)
            if data is not None:
                stock_technical_index.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in codes:

        QA_util_log_info('The {} of Total {}'.format
                         ((codes.index(item) +1), len(codes)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(item) +1) / len(codes) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((codes.index(item) +1) / len(codes) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

        __saving_work( item,start_date,end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock_technical_index half ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_technical_week_half(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = QA_util_get_pre_trade_date(QA_util_today_str(),3)
        else:
            start_date = QA_util_get_pre_trade_date(end_date,3)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,3)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = list(QA_fetch_stock_om_all()['code'])
    else:
        codes = QA_util_code_tolist(codes)

    stock_technical_week = client.stock_technical_week_half
    stock_technical_week.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_week half from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator_half(code, start_date, end_date, type='week')
            if data is not None:
                stock_technical_week.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in codes:

        QA_util_log_info('The {} of Total {}'.format
                         ((codes.index(item) +1), len(codes)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(item) +1) / len(codes) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((codes.index(item) +1) / len(codes) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

        __saving_work( item,start_date,end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock_technical_week half ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_stock_technical_week_half_his(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = "2006-01-01"
        else:
            start_date = QA_util_get_pre_trade_date(end_date,3)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,3)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = list(QA_fetch_stock_all()['code'])
    else:
        codes = QA_util_code_tolist(codes)

    stock_technical_week = client.stock_technical_week_half
    stock_technical_week.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_week half from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator_half(code, start_date, end_date, type='week')
            if data is not None:
                stock_technical_week.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in codes:

        QA_util_log_info('The {} of Total {}'.format
                         ((codes.index(item) +1), len(codes)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(item) +1) / len(codes) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((codes.index(item) +1) / len(codes) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

        __saving_work( item,start_date,end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock_technical_week half ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_technical_index_real(codes=None, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = QA_util_get_pre_trade_date(QA_util_today_str(),3)
        else:
            start_date = QA_util_get_pre_trade_date(end_date,3)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,3)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = list(QA_fetch_stock_om_all()['code'])
    else:
        codes = QA_util_code_tolist(codes)

    stock_technical_index = client.stock_technical_index_real
    stock_technical_index.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_half real from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator_halfreal(code, start_date, end_date,'day')
            if data is not None:
                stock_technical_index.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in codes:

        QA_util_log_info('The {} of Total {}'.format
                         ((codes.index(item) +1), len(codes)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(item) +1) / len(codes) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((codes.index(item) +1) / len(codes) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

        __saving_work( item,start_date,end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock_technical_index real half ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_stock_technical_week_real(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = "2006-01-01"
        else:
            start_date = QA_util_get_pre_trade_date(end_date,3)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,3)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = list(QA_fetch_stock_all()['code'])
    else:
        codes = QA_util_code_tolist(codes)

    stock_technical_week = client.stock_technical_week_real
    stock_technical_week.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_week real from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator_halfreal(code, start_date, end_date, type='week')
            if data is not None:
                stock_technical_week.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in codes:

        QA_util_log_info('The {} of Total {}'.format
                         ((codes.index(item) +1), len(codes)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(item) +1) / len(codes) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((codes.index(item) +1) / len(codes) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

        __saving_work( item,start_date,end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock_technical_week real ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)