
import pymongo
from QUANTAXIS.QAUtil import (DATABASE, QA_util_getBetweenQuarter, QA_util_log_info, QA_util_add_months,
                              QA_util_to_json_from_pandas, QA_util_today_str,QA_util_get_pre_trade_date,
                              QA_util_datetime_to_strdate,QA_util_code_tolist,QA_util_get_trade_range)
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_all,QA_fetch_stock_om_all
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_indicator,QA_fetch_get_index_indicator
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_index_list_adv

def QA_SU_save_stock_technical_index_day(codes=None, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = QA_util_get_pre_trade_date(QA_util_today_str(),1)
        else:
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_stock_om_all().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    stock_technical_index = client.stock_technical_index
    stock_technical_index.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_index from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator(code, start_date, end_date,'day')
            if data is not None:
                stock_technical_index.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    date_rng = QA_util_get_trade_range(start_date, end_date)

    if len(date_rng) > 10:
        for item in codes:

            QA_util_log_info('The {} of Total {}'.format
                             ((codes.index(item) +1), len(codes)))

            strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(item) +1) / len(codes) * 100))[0:4] + '%', ui_log)
            intProgressToLog = int(float((codes.index(item) +1) / len(codes) * 100))
            QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

            __saving_work( item,start_date,end_date)
    else:
        k=100
        for i in range(0, len(codes), k):
            code = codes[i:i+k]
            QA_util_log_info('The {} of Total {}'.format
                             ((i +k ), len(codes)))
            strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((i + k) / len(codes) * 100))[0:4] + '%', ui_log)
            intProgressToLog = int(float((i + k ) / len(codes) * 100 ))
            QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)
            __saving_work(code, start_date, end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock_technical_index ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_technical_index_his(codes=None, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

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
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_stock_all().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    stock_technical_index = client.stock_technical_index
    stock_technical_index.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_index from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator(code, start_date, end_date)
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
        QA_util_log_info('SUCCESS save stock_technical_index ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_technical_week_day(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = end_date
        else:
            start_date = end_date
    else:
        start_date = start_date
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_stock_om_all().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    #client.drop_collection('stock_technical_week')
    stock_technical_week = client.stock_technical_week
    stock_technical_week.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_week from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator(code, start_date, end_date, type='week')
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
        QA_util_log_info('SUCCESS save stock_technical_week ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_stock_technical_week_his(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

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
            start_date = end_date
    else:
        start_date = start_date
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_stock_all().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    #client.drop_collection('stock_technical_week')
    stock_technical_week = client.stock_technical_week
    stock_technical_week.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_week from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator(code, start_date, end_date, type='week')
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
        QA_util_log_info('SUCCESS save stock_technical_week ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_technical_month_day(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = end_date
        else:
            start_date = end_date
    else:
        start_date = start_date
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_stock_om_all().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    #client.drop_collection('stock_technical_month')
    stock_technical_month = client.stock_technical_month
    stock_technical_month.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_month from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator(code, start_date, end_date, type='month')
            if data is not None:
                stock_technical_month.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save stock_technical_month ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_stock_technical_month_his(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

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
            start_date = end_date
    else:
        start_date = start_date
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_stock_all().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    #client.drop_collection('stock_technical_month')
    stock_technical_month = client.stock_technical_month
    stock_technical_month.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_month from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator(code, start_date, end_date, type='month')
            if data is not None:
                stock_technical_month.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save stock_technical_month ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_index_technical_index_day(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = QA_util_get_pre_trade_date(QA_util_today_str(),1)
        else:
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_index_list_adv().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    index_technical_index = client.index_technical_index
    index_technical_index.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving index_technical_index from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_index_indicator(code, start_date, end_date,'day')
            if data is not None:
                index_technical_index.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save index_technical_index ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_index_technical_index_his(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

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
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_index_list_adv().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    index_technical_index = client.index_technical_index
    index_technical_index.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving index_technical_index from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_index_indicator(code, start_date, end_date)
            if data is not None:
                index_technical_index.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save index_technical_index ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_index_technical_week_day(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = end_date
        else:
            start_date = end_date
    else:
        start_date = start_date
        if end_date == None:
            end_date = QA_util_today_str()
    if codes is None:
        codes = QA_fetch_index_list_adv().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    #client.drop_collection('index_technical_week')
    index_technical_week = client.index_technical_week
    index_technical_week.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving index_technical_week from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_index_indicator(code, start_date, end_date, type='week')
            if data is not None:
                index_technical_week.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save index_technical_week ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_index_technical_week_his(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

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
            start_date = end_date
    else:
        start_date = start_date
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_index_list_adv().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    #client.drop_collection('index_technical_week')
    index_technical_week = client.index_technical_week
    index_technical_week.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving index_technical_week from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_index_indicator(code, start_date, end_date, type='week')
            if data is not None:
                index_technical_week.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save index_technical_week ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_index_technical_month_day(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = end_date
        else:
            start_date = end_date
    else:
        start_date = start_date
        if end_date == None:
            end_date = QA_util_today_str()
    if codes is None:
        codes = QA_fetch_index_list_adv().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    #client.drop_collection('index_technical_month')
    index_technical_month = client.index_technical_month
    index_technical_month.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving index_technical_month from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_index_indicator(code, start_date, end_date, type='month')
            if data is not None:
                index_technical_month.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save index_technical_month ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_index_technical_month_his(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

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
            start_date = end_date
    else:
        start_date = start_date
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_index_list_adv().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    #client.drop_collection('index_technical_month')
    index_technical_month = client.index_technical_month
    index_technical_month.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving index_technical_month from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_index_indicator(code, start_date, end_date, type='month')
            if data is not None:
                index_technical_month.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save index_technical_month ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_technical_hour_day(codes=None, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = QA_util_get_pre_trade_date(QA_util_today_str(),1)
        else:
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_stock_om_all().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    stock_technical_hour = client.stock_technical_hour
    stock_technical_hour.create_index([('code',
                                        pymongo.ASCENDING),
                                       ('time_stamp',
                                        pymongo.ASCENDING),
                                       ('date_stamp',
                                        pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_hour from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator(code, start_date, end_date, type='hour')
            if data is not None:
                stock_technical_hour.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    date_rng = QA_util_get_trade_range(start_date, end_date)

    if len(date_rng) > 10:
        for item in codes:

            QA_util_log_info('The {} of Total {}'.format
                             ((codes.index(item) +1), len(codes)))

            strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(item) +1) / len(codes) * 100))[0:4] + '%', ui_log)
            intProgressToLog = int(float((codes.index(item) +1) / len(codes) * 100))
            QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

            __saving_work( item,start_date,end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock_technical_hour ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_technical_hour_his(codes=None, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = "2010-01-01"
        else:
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_stock_all().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    stock_technical_hour = client.stock_technical_hour
    stock_technical_hour.create_index([('code',
                                        pymongo.ASCENDING),
                                       ('time_stamp',
                                        pymongo.ASCENDING),
                                       ('date_stamp',
                                        pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_hour from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator(code, start_date, end_date, type='hour')
            if data is not None:
                stock_technical_hour.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    date_rng = QA_util_get_trade_range(start_date, end_date)

    if len(date_rng) > 10:
        for item in codes:

            QA_util_log_info('The {} of Total {}'.format
                             ((codes.index(item) +1), len(codes)))

            strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(item) +1) / len(codes) * 100))[0:4] + '%', ui_log)
            intProgressToLog = int(float((codes.index(item) +1) / len(codes) * 100))
            QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

            __saving_work( item,start_date,end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock_technical_hour ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_index_technical_hour_day(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = QA_util_get_pre_trade_date(QA_util_today_str(),1)
        else:
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_index_list_adv().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    index_technical_hour = client.index_technical_hour
    index_technical_hour.create_index([('code',
                                        pymongo.ASCENDING),
                                       ('time_stamp',
                                        pymongo.ASCENDING),
                                       ('date_stamp',
                                        pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving index_technical_hour from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_index_indicator(code, start_date, end_date, 'hour')
            if data is not None:
                index_technical_hour.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    date_rng = QA_util_get_trade_range(start_date, end_date)

    if len(date_rng) > 10:
        for item in codes:

            QA_util_log_info('The {} of Total {}'.format
                             ((codes.index(item) +1), len(codes)))

            strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(item) +1) / len(codes) * 100))[0:4] + '%', ui_log)
            intProgressToLog = int(float((codes.index(item) +1) / len(codes) * 100))
            QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

            __saving_work( item,start_date,end_date)
    else:
        k=100
        for i in range(0, len(codes), k):
            code = codes[i:i+k]
            QA_util_log_info('The {} of Total {}'.format
                             ((i +k ), len(codes)))
            strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((i + k) / len(codes) * 100))[0:4] + '%', ui_log)
            intProgressToLog = int(float((i + k ) / len(codes) * 100 ))
            QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)
            __saving_work(code, start_date, end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save index_technical_hour ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_index_technical_hour_his(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = "2010-01-01"
        else:
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_index_list_adv().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    index_technical_hour = client.index_technical_hour
    index_technical_hour.create_index([('code',
                                        pymongo.ASCENDING),
                                       ('time_stamp',
                                        pymongo.ASCENDING),
                                       ('date_stamp',
                                        pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving index_technical_hour from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_index_indicator(code, start_date, end_date, 'hour')
            if data is not None:
                index_technical_hour.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    date_rng = QA_util_get_trade_range(start_date, end_date)

    if len(date_rng) > 10:
        for item in codes:

            QA_util_log_info('The {} of Total {}'.format
                             ((codes.index(item) +1), len(codes)))

            strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((codes.index(item) +1) / len(codes) * 100))[0:4] + '%', ui_log)
            intProgressToLog = int(float((codes.index(item) +1) / len(codes) * 100))
            QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

            __saving_work( item,start_date,end_date)
    else:
        k=100
        for i in range(0, len(codes), k):
            code = codes[i:i+k]
            QA_util_log_info('The {} of Total {}'.format
                             ((i +k ), len(codes)))
            strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((i + k) / len(codes) * 100))[0:4] + '%', ui_log)
            intProgressToLog = int(float((i + k ) / len(codes) * 100 ))
            QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)
            __saving_work(code, start_date, end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save index_technical_hour ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_stock_technical_15min_day(codes=None, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = QA_util_get_pre_trade_date(QA_util_today_str(),1)
        else:
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_stock_om_all().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    stock_technical_hour = client.stock_technical_15min
    stock_technical_hour.create_index([('code',
                                        pymongo.ASCENDING),
                                       ('time_stamp',
                                        pymongo.ASCENDING),
                                       ('date_stamp',
                                        pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_15min from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator(code, start_date, end_date, type='min')
            if data is not None:
                stock_technical_hour.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save stock_technical_15min ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_technical_15min_his(codes=None, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = "2010-01-01"
        else:
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_stock_all().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    stock_technical_hour = client.stock_technical_15min
    stock_technical_hour.create_index([('code',
                                        pymongo.ASCENDING),
                                       ('time_stamp',
                                        pymongo.ASCENDING),
                                       ('date_stamp',
                                        pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_15min from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator(code, start_date, end_date, type='min')
            if data is not None:
                stock_technical_hour.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save stock_technical_15min ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_technical_30min_day(codes=None, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = QA_util_get_pre_trade_date(QA_util_today_str(),1)
        else:
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_stock_om_all().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    stock_technical_hour = client.stock_technical_30min
    stock_technical_hour.create_index([('code',
                                        pymongo.ASCENDING),
                                       ('time_stamp',
                                        pymongo.ASCENDING),
                                       ('date_stamp',
                                        pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_30min from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator(code, start_date, end_date, type='min')
            if data is not None:
                stock_technical_hour.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save stock_technical_30min ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_technical_30min_his(codes=None, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = "2010-01-01"
        else:
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_stock_all().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    stock_technical_hour = client.stock_technical_30min
    stock_technical_hour.create_index([('code',
                                        pymongo.ASCENDING),
                                       ('time_stamp',
                                        pymongo.ASCENDING),
                                       ('date_stamp',
                                        pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_30min from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_stock_indicator(code, start_date, end_date, type='min')
            if data is not None:
                stock_technical_hour.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save stock_technical_30min ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_index_technical_15min_day(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = QA_util_get_pre_trade_date(QA_util_today_str(),1)
        else:
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_index_list_adv().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    index_technical_hour = client.index_technical_15min
    index_technical_hour.create_index([('code',
                                        pymongo.ASCENDING),
                                       ('time_stamp',
                                        pymongo.ASCENDING),
                                       ('date_stamp',
                                        pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving index_technical_15min from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_index_indicator(code, start_date, end_date, 'min')
            if data is not None:
                index_technical_hour.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save index_technical_15min ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_index_technical_15min_his(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = "2010-01-01"
        else:
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_index_list_adv().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    index_technical_hour = client.index_technical_15min
    index_technical_hour.create_index([('code',
                                        pymongo.ASCENDING),
                                       ('time_stamp',
                                        pymongo.ASCENDING),
                                       ('date_stamp',
                                        pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving index_technical_15min from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_index_indicator(code, start_date, end_date, 'min')
            if data is not None:
                index_technical_hour.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save index_technical_15min ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_index_technical_30min_day(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = QA_util_get_pre_trade_date(QA_util_today_str(),1)
        else:
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_index_list_adv().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    index_technical_hour = client.index_technical_30min
    index_technical_hour.create_index([('code',
                                        pymongo.ASCENDING),
                                       ('time_stamp',
                                        pymongo.ASCENDING),
                                       ('date_stamp',
                                        pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving index_technical_30min from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_index_indicator(code, start_date, end_date, 'min')
            if data is not None:
                index_technical_hour.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save index_technical_30min ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_index_technical_30min_his(codes = None,start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

    '''
     save stock_day
    计算技术指标
    历史全部数据
    :return:
    '''
    if start_date == None:
        if end_date == None:
            end_date = QA_util_today_str()
            start_date = "2010-01-01"
        else:
            start_date = QA_util_get_pre_trade_date(end_date,1)
    else:
        start_date = QA_util_get_pre_trade_date(start_date,1)
        if end_date == None:
            end_date = QA_util_today_str()

    if codes is None:
        codes = QA_fetch_index_list_adv().code.unique().tolist()
    else:
        codes = QA_util_code_tolist(codes)

    index_technical_hour = client.index_technical_30min
    index_technical_hour.create_index([('code',
                                        pymongo.ASCENDING),
                                       ('time_stamp',
                                        pymongo.ASCENDING),
                                       ('date_stamp',
                                        pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving index_technical_30min from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_index_indicator(code, start_date, end_date, 'min')
            if data is not None:
                index_technical_hour.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
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
        QA_util_log_info('SUCCESS save index_technical_30min ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

if __name__ == '__main__':
    pass