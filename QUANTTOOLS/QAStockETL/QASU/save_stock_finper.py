
import pymongo
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_financial_percent
from QUANTTOOLS.QAStockETL.QAUtil import ASCENDING
from QUANTAXIS.QAUtil import (DATABASE, QA_util_to_json_from_pandas, QA_util_today_str,QA_util_code_tolist,QA_util_log_info,
                              QA_util_get_trade_range)
import pandas as pd
from QUANTAXIS.QAFetch.QAQuery_Advance import (QA_fetch_stock_list_adv, QA_fetch_stock_block_adv,
                                               QA_fetch_stock_day_adv)


def QA_SU_save_stock_fianacial_percent(code, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    if code is None:
        code = list(QA_fetch_stock_list_adv()['code'])
    else:
        code = QA_util_code_tolist(code)

    if start_date is None:
        if end_date is None:
            start_date = QA_util_today_str()
            end_date = start_date
        elif end_date is not None:
            start_date = '2008-01-01'
    elif start_date is not None:
        if end_date == None:
            end_date = QA_util_today_str()
        elif end_date is not None:
            if end_date < start_date:
                print('end_date should large than start_date')

    stock_financial_percent = DATABASE.stock_financial_percent
    stock_financial_percent.create_index(
        [("code", ASCENDING), ("date_stamp", ASCENDING)], unique=True)
    err = []

    def __saving_work(code,START_DATE,END_DATE, stock_financial_percent):
        try:
            QA_util_log_info(
                '##JOB01 Now Saving stock_technical_index from {START_DATE} to {END_DATE} ==== {code}'.format(code=str(code),START_DATE=START_DATE,END_DATE=END_DATE), ui_log)
            data = QA_fetch_get_stock_financial_percent(code, START_DATE, END_DATE)
            data = data.drop_duplicates(
                (['code', 'date']))
            print("got from '{from_}' to '{to_}' stock financial percent data.".format(from_ =start_date, to_ = end_date))
            if data is not None:
                stock_financial_percent.insert_many(data = QA_util_to_json_from_pandas(data), ordered=False)
        except Exception as error0:
            print(error0)
            err.append(str(code))

    for item in code:

        QA_util_log_info('The {} of Total {}'.format
                         ((code.index(item) +1), len(code)))

        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((code.index(item) +1) / len(code) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((code.index(item) +1) / len(code) * 100))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)
        __saving_work( item,start_date, end_date, stock_financial_percent)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save stock_technical_index ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)