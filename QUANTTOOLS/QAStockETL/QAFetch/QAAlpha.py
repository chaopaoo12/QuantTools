from QUANTTOOLS.QAStockETL.QAUtil import stock_alpha, index_alpha, stock_alpha101, index_alpha101
from QUANTAXIS.QAUtil import QA_util_date_stamp,QA_util_if_trade,QA_util_log_info,QA_util_get_trade_range

def QA_fetch_get_stock_alpha(code, date, ui_log = None):
    if QA_util_if_trade(date) == True:
        data = stock_alpha(code, date).reset_index()
        names = list(data.columns)
        names[0] = 'code'
        data.columns = names
        data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
        return(data)
    else:
        QA_util_log_info(
            '##JOB Non Data Stock Alpha191 for ============== {date}'.format(date), ui_log)

def QA_fetch_get_index_alpha(code, date, ui_log = None):
    if QA_util_if_trade(date) == True:
        data = index_alpha(code, date).reset_index()
        names = list(data.columns)
        names[0] = 'code'
        data.columns = names
        data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
        return(data)
    else:
        QA_util_log_info(
            '##JOB Non Data Index Alpha191 for ============== {date}'.format(date), ui_log)

def QA_fetch_get_stock_alpha101(code, start, end, ui_log = None):
    deal_date_list = QA_util_get_trade_range(start, end)
    if deal_date_list is not None:
        data = stock_alpha101(code, start, end)
        names = list(data.columns)
        data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
        return(data)
    else:
        QA_util_log_info(
            '##JOB Non Data Stock Alpha101 ============== from {_from} to {_to}'.format(start, end), ui_log)

def QA_fetch_get_index_alpha101(code, start, end, ui_log = None):
    deal_date_list = QA_util_get_trade_range(start, end)
    if deal_date_list is not None:
        data = index_alpha101(code, start, end)
        data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
        return(data)
    else:
        QA_util_log_info(
            '##JOB Non Data Index Alpha101 ============== from {_from} to {_to}'.format(start, end), ui_log)