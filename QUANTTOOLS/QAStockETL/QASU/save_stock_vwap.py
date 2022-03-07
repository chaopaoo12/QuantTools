
import pymongo
from QUANTAXIS.QAUtil import (DATABASE, QA_util_getBetweenQuarter, QA_util_log_info, QA_util_add_months,
                              QA_util_to_json_from_pandas, QA_util_today_str,QA_util_get_pre_trade_date,
                              QA_util_datetime_to_strdate,QA_util_code_tolist)
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_all,QA_fetch_stock_om_all
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_vwap

def QA_SU_save_stock_vwap_day(codes=None, start_date=None,end_date=None, client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    计算vwap指标
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

    stock_vwap = client.stock_vwap
    stock_vwap.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving save_stock_vwap from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_vwap(code, start_date, end_date)
            if data is not None:
                stock_vwap.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
                QA_util_log_info(
                    '##JOB01 Now Saving save_stock_vwap Success {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    k=500
    for i in range(0, len(codes), k):
        code = codes[i:i+k]
        QA_util_log_info('The {} of Total {}'.format
                         ((i +k ), len(codes)))
        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((i + k) / len(codes) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((i + k ) / len(codes) * 100 ))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)
        __saving_work(code,start_date,end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save save_stock_vwap ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_stock_vwap_his(codes=None, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):

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

    stock_vwap = client.stock_vwap
    stock_vwap.create_index([("code", pymongo.ASCENDING),("date_stamp", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code,start_date,end_date):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving save_stock_vwap from {start_date} to {end_date} ==== {code}'.format(code=str(code),start_date=start_date,end_date=end_date), ui_log)
            data = QA_fetch_get_vwap(code, start_date, end_date)
            if data is not None:
                stock_vwap.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    k=100
    for i in range(0, len(codes), k):
        code = codes[i:i+k]
        QA_util_log_info('The {} of Total {}'.format
                         ((i +k ), len(codes)))
        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((i + k) / len(codes) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((i + k ) / len(codes) * 100 ))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)
        __saving_work(code,start_date,end_date)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save save_stock_vwap ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)