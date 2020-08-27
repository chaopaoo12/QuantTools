from QUANTAXIS.QAUtil import (DATABASE, QA_util_log_info,QA_util_to_json_from_pandas,QA_util_today_str)
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_all
from QUANTTOOLS.QAStockETL.QAFetch.QAFinancial import QA_fetch_get_stock_report_ths
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_financial_calendar_adv
import pymongo
import gc

def QA_SU_save_financial_report_day(client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    保存财报日历
    历史全部数据
    :return:
    '''
    code = list(QA_fetch_stock_financial_calendar_adv(list(QA_fetch_stock_all()['code']),QA_util_today_str()).data['code'])
    stock_financial = client.stock_financial
    stock_financial.create_index([("code", pymongo.ASCENDING), ("report_date", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code, stock_financial):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving THS financial_report==== {}'.format(str(code)), ui_log)

            stock_financial.insert_many(QA_util_to_json_from_pandas(
                QA_fetch_get_stock_report_ths(code)), ordered=False)
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

        __saving_work( item, stock_financial)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save THS financial_report ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)


def QA_SU_save_financial_report_his(client=DATABASE, ui_log = None, ui_progress = None):
    '''
    save stock_day
    保存财报日历
    反向查询四个季度财报
    :return:
    '''
    code = list(QA_fetch_stock_all()['code'])
    stock_financial = client.stock_financial
    stock_financial.create_index([("code", pymongo.ASCENDING), ("report_date", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code, stock_financial):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving THS financial_report==== {}'.format(str(code)), ui_log)
            stock_financial.insert_many(QA_util_to_json_from_pandas(
                QA_fetch_get_stock_report_ths(code)), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in code:
        QA_util_log_info('The {} of Total {}'.format
                         ((code.index(item) +1), len(code)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((code.index(item) +1) / len(code) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((code.index(item) + 1)/ len(code) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

        __saving_work( item, stock_financial)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save THS financial_report ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)