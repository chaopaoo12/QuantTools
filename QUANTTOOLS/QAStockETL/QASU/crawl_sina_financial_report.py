from QUANTAXIS.QAUtil import (DATABASE, QA_util_log_info,QA_util_to_json_from_pandas,QA_util_today_str,QA_util_datetime_to_strdate)
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_list_adv
from QUANTTOOLS.QAStockETL.QAFetch.QAFinancial import QA_fetch_get_stock_report_sina
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_financial_calendar_adv
from QUANTTOOLS.QAStockETL.QAUtil import QA_util_add_days, QA_util_add_years, QA_util_getBetweenYear
import pymongo
import gc

def QA_SU_save_financial_report_day(client=DATABASE, ui_log = None, ui_progress = None):
    '''
     save stock_day
    保存财报日历
    历史全部数据
    :return:
    '''
    END_DATE = QA_util_today_str()
    START_DATE = QA_util_datetime_to_strdate(QA_util_add_days(QA_util_today_str(),-7))

    END_YEAR = QA_util_today_str()[0:4]
    START_YEAR = QA_util_datetime_to_strdate(QA_util_add_years(QA_util_today_str(),-1))[0:4]

    YEARS = [END_YEAR, START_YEAR]

    code = list(QA_fetch_stock_financial_calendar_adv(list(QA_fetch_stock_list_adv()['code']),START_DATE, END_DATE).data['code'])
    stock_financial_sina = client.stock_financial_sina
    stock_financial_sina.create_index([("code", pymongo.ASCENDING), ("report_date", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code, stock_financial_sina):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving THS financial_report==== {}'.format(str(code)), ui_log)

            stock_financial_sina.insert_many(QA_util_to_json_from_pandas(
                QA_fetch_get_stock_report_sina(code, YEARS)), ordered=False)
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

        __saving_work( item, stock_financial_sina)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save SINA financial_report ^_^',  ui_log)
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
    YEARS = list(QA_util_getBetweenYear('2002-01-01',QA_util_today_str()).values())

    code = list(QA_fetch_stock_list_adv()['code'])
    stock_financial_sina = client.stock_financial_sina
    stock_financial_sina.create_index([("code", pymongo.ASCENDING), ("report_date", pymongo.ASCENDING)], unique=True)
    err = []

    def __saving_work(code, stock_financial_sina):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving THS financial_report==== {}'.format(str(code)), ui_log)
            stock_financial_sina.insert_many(QA_util_to_json_from_pandas(
                QA_fetch_get_stock_report_sina(code, YEARS)), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in code:
        QA_util_log_info('The {} of Total {}'.format
                         ((code.index(item) +1), len(code)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((code.index(item) +1) / len(code) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((code.index(item) + 1)/ len(code) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)

        __saving_work( item, stock_financial_sina)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save SINA financial_report ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)