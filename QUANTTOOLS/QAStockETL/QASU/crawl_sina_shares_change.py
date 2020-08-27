from QUANTAXIS.QAUtil import (DATABASE, QA_util_log_info,QA_util_to_json_from_pandas,QA_util_today_str,QA_util_datetime_to_strdate)
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_all
from QUANTTOOLS.QAStockETL.QAFetch.QAShares import QA_fetch_get_stock_shares_sina
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_financial_calendar_adv
from QUANTTOOLS.QAStockETL.QAUtil import QA_util_add_days, QA_util_getBetweenYear
import pymongo
import gc

def QA_SU_save_stock_shares_day(client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    保存财报日历
    历史全部数据
    :return:
    '''
    END_DATE = QA_util_today_str()
    START_DATE = QA_util_datetime_to_strdate(QA_util_add_days(QA_util_today_str(),-7))

    code = list(QA_fetch_stock_all()['code'])
    stock_shares = client.stock_shares
    stock_shares.create_index([("code", pymongo.ASCENDING), ("begin_date", pymongo.ASCENDING),
                               ('total_shares', pymongo.DESCENDING), ('reason', pymongo.DESCENDING)
                                  , ('send_date', pymongo.DESCENDING)], unique=True)
    err = []

    def __saving_work(code, stock_shares):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving SSINA shares change==== {}'.format(str(code)), ui_log)

            stock_shares.insert_many(QA_util_to_json_from_pandas(
                QA_fetch_get_stock_shares_sina(code)), ordered=False)
            gc.collect()
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in code:

        QA_util_log_info('The {} of Total {}'.format
                         ((code.index(item) +1), len(code)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((code.index(item) +1) / len(code) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((code.index(item) +1) / len(code) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

        __saving_work( item, stock_shares)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save SINA shares change ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_stock_shares_his(client=DATABASE, ui_log = None, ui_progress = None):
    '''
    save stock_day
    保存财报日历
    反向查询四个季度财报
    :return:
    '''
    code = list(QA_fetch_stock_all()['code'])
    stock_shares = client.stock_shares
    stock_shares.create_index([("code", pymongo.ASCENDING), ("begin_date", pymongo.ASCENDING), ('total_shares', pymongo.DESCENDING)], unique=True)
    err = []

    def __saving_work(code, stock_shares):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving SINA shares change==== {}'.format(str(code)), ui_log)
            stock_shares.insert_many(QA_util_to_json_from_pandas(
                QA_fetch_get_stock_shares_sina(code)), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in code:
        QA_util_log_info('The {} of Total {}'.format
                         ((code.index(item) +1), len(code)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((code.index(item) +1) / len(code) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((code.index(item) + 1)/ len(code) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

        __saving_work( item, stock_shares)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save SINA shares change ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)