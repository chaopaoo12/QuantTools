
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_usstock_financial_percent
from QUANTTOOLS.QAStockETL.QAUtil import ASCENDING
from QUANTAXIS.QAUtil import (DATABASE, QA_util_to_json_from_pandas, QA_util_today_str,QA_util_code_tolist,QA_util_log_info)
from QUANTTOOLS.QAStockETL.QAUtil import (QA_util_get_trade_range,QA_util_get_pre_trade_date)
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_usstock_list


def QA_SU_save_usstock_fianacial_percent(code = None, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    if code is None:
        codes = list(QA_fetch_usstock_list()['code'])
    else:
        codes = QA_util_code_tolist(code)

    if start_date is None:
        if end_date is None:
            start_date = QA_util_get_pre_trade_date(QA_util_today_str(),1,'us')
            end_date = QA_util_today_str()
        elif end_date is not None:
            start_date = '2008-01-01'
    elif start_date is not None:
        if end_date == None:
            end_date = QA_util_today_str()
        elif end_date is not None:
            if end_date < start_date:
                QA_util_log_info('end_date should large than start_date start {_from} end {_to} '.format(_from=start_date, _to=end_date), ui_log)

    stock_financial_percent = DATABASE.usstock_financial_percent
    stock_financial_percent.create_index(
        [("code", ASCENDING), ("date_stamp", ASCENDING)], unique=True)
    err = []

    def __saving_work(code,START_DATE,END_DATE, stock_financial_percent):
        try:
            QA_util_log_info(
                '##JOB01 Pre Data usstock_fianacial_percent from {START_DATE} to {END_DATE} '.format(START_DATE=START_DATE,END_DATE=END_DATE), ui_log)
            data = QA_fetch_get_usstock_financial_percent(code, START_DATE, END_DATE)
            data = data.drop_duplicates(
                (['code', 'date']))
            QA_util_log_info(
                '##JOB02 Got Data usstock_fianacial_percent from {START_DATE} to {END_DATE} '.format(START_DATE=START_DATE,END_DATE=END_DATE), ui_log)
            if data is not None:
                stock_financial_percent.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
                QA_util_log_info(
                    '##JOB03 Now usstock_fianacial_percent saved from {START_DATE} to {END_DATE} '.format(START_DATE=START_DATE,END_DATE=END_DATE), ui_log)
            else:
                QA_util_log_info(
                    '##JOB01 No Data usstock_fianacial_percent from {START_DATE} to {END_DATE} '.format(START_DATE=START_DATE,END_DATE=END_DATE), ui_log)
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
        __saving_work( code, start_date, end_date, stock_financial_percent)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save usstock_fianacial_percent ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)

def QA_SU_save_usstock_fianacial_percent_his(code = None, start_date=None,end_date=None,client=DATABASE, ui_log = None, ui_progress = None):
    if code is None:
        codes = list(QA_fetch_usstock_list()['code'])
    else:
        codes = QA_util_code_tolist(code)

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
                QA_util_log_info('end_date should large than start_date start {_from} end {_to} '.format(_from=start_date, _to=end_date), ui_log)

    stock_financial_percent = DATABASE.usstock_financial_percent
    stock_financial_percent.create_index(
        [("code", ASCENDING), ("date_stamp", ASCENDING)], unique=True)
    err = []

    def __saving_work(code,START_DATE,END_DATE, stock_financial_percent):
        try:
            QA_util_log_info(
                '##JOB01 Pre Data usstock_fianacial_percent from {START_DATE} to {END_DATE} '.format(START_DATE=START_DATE,END_DATE=END_DATE), ui_log)
            data = QA_fetch_get_usstock_financial_percent(code, START_DATE, END_DATE)
            data = data.drop_duplicates(
                (['code', 'date']))
            QA_util_log_info(
                '##JOB02 Got Data usstock_fianacial_percent from {START_DATE} to {END_DATE} '.format(START_DATE=START_DATE,END_DATE=END_DATE), ui_log)
            if data is not None:
                stock_financial_percent.insert_many(QA_util_to_json_from_pandas(data), ordered=False)
                QA_util_log_info(
                    '##JOB03 Now usstock_fianacial_percent saved from {START_DATE} to {END_DATE} '.format(START_DATE=START_DATE,END_DATE=END_DATE), ui_log)
            else:
                QA_util_log_info(
                    '##JOB01 No Data usstock_fianacial_percent from {START_DATE} to {END_DATE} '.format(START_DATE=START_DATE,END_DATE=END_DATE), ui_log)
        except Exception as error0:
            print(error0)
            err.append(str(code))
    k=250
    for i in range(0, len(codes), k):
        code = codes[i:i+k]
        QA_util_log_info('The {} of Total {}'.format
                         ((i +k ), len(codes)))
        strProgressToLog = 'DOWNLOAD PROGRESS {}'.format(str(float((i + k) / len(codes) * 100))[0:4] + '%', ui_log)
        intProgressToLog = int(float((i + k ) / len(codes) * 100 ))
        QA_util_log_info(strProgressToLog, ui_log= ui_log, ui_progress= ui_progress, ui_progress_int_value= intProgressToLog)
        __saving_work( code, start_date, end_date, stock_financial_percent)

    if len(err) < 1:
        QA_util_log_info('SUCCESS save usstock_fianacial_percent ^_^',  ui_log)
    else:
        QA_util_log_info(' ERROR CODE \n ',  ui_log)
        QA_util_log_info(err, ui_log)