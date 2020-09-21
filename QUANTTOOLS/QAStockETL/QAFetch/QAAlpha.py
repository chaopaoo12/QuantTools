from QUANTTOOLS.QAStockETL.QAFetch.AlphaTools import (stock_alpha, index_alpha, stock_alpha101, index_alpha101,
                                                      stock_alpha101_half, stock_alpha101_half_realtime)
from QUANTAXIS.QAUtil import QA_util_date_stamp,QA_util_if_trade,QA_util_log_info,QA_util_get_trade_range,QA_util_today_str,QA_util_get_real_date

def QA_fetch_get_stock_alpha(code, date, ui_log = None):
    if QA_util_if_trade(date) == True:
        data = stock_alpha(code, date).reset_index()
        if data is not None:
            names = list(data.columns)
            names[0] = 'code'
            data.columns = names
            data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
            return(data)
        else:
            QA_util_log_info(
                '##JOB Non Data Stock Alpha191 for ============== {}'.format(date), ui_log)
    else:
        QA_util_log_info(
            '##JOB Non Data Stock Alpha191 for ============== {}'.format(date), ui_log)

def QA_fetch_get_index_alpha(code, date, ui_log = None):
    if QA_util_if_trade(date) == True:
        data = index_alpha(code, date).reset_index()
        if data is not None:
            names = list(data.columns)
            names[0] = 'code'
            data.columns = names
            data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
            return(data)
        else:
            QA_util_log_info(
                '##JOB Non Data Index Alpha191 for ============== {}'.format(date), ui_log)
    else:
        QA_util_log_info(
            '##JOB Non Data Index Alpha191 for ============== {}'.format(date), ui_log)

def QA_fetch_get_stock_alpha101(code, start, end, ui_log = None):
    deal_date_list = QA_util_get_trade_range(start, end)
    if deal_date_list is not None:
        data = stock_alpha101(code, start, end)
        if data is not None:
            data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
            return(data)
        else:
            QA_util_log_info(
                '##JOB Non Data Stock Alpha101 ============== from {_from} to {_to}'.format(_from=start, _to=end), ui_log)
    else:
        QA_util_log_info(
            '##JOB Non Data Stock Alpha101 ============== from {_from} to {_to}'.format(_from=start, _to=end), ui_log)

def QA_fetch_get_index_alpha101(code, start, end, ui_log = None):
    deal_date_list = QA_util_get_trade_range(start, end)
    if deal_date_list is not None:
        data = index_alpha101(code, start, end)
        if data is not None:
            data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
            return(data)
        QA_util_log_info(
            '##JOB Non Data Index Alpha101 ============== from {_from} to {_to}'.format(_from=start, _to=end), ui_log)
    else:
        QA_util_log_info(
            '##JOB Non Data Index Alpha101 ============== from {_from} to {_to}'.format(_from=start, _to=end), ui_log)

def QA_fetch_get_stock_alpha101_half(code, start, end, ui_log = None):
    deal_date_list = QA_util_get_trade_range(start, end)
    if deal_date_list is not None:
        data = stock_alpha101_half(code, start, end)
        if data is not None:
            data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
            return(data)
        else:
            QA_util_log_info(
                '##JOB Non Data Stock Alpha101 ============== from {_from} to {_to}'.format(_from=start, _to=end), ui_log)
    else:
        QA_util_log_info(
            '##JOB Non Data Stock Alpha101 ============== from {_from} to {_to}'.format(_from=start, _to=end), ui_log)

def QA_fetch_get_stock_alpha101half_realtime(code, start = None, end = None, ui_log = None):

    end =QA_util_today_str()

    if QA_util_if_trade(end):
        pass
    else:
        end = QA_util_get_real_date(end)

    if start is None:
        start = end

    deal_date_list = QA_util_get_trade_range(start, end)
    if deal_date_list is not None:
        data = stock_alpha101_half_realtime(code, start, end).reset_index()
        if data is not None:
            data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
            return(data)
        else:
            QA_util_log_info(
                '##JOB Non Data Stock Alpha101 ============== from {_from} to {_to}'.format(_from=start, _to=end), ui_log)
    else:
        QA_util_log_info(
            '##JOB Non Data Stock Alpha101 ============== from {_from} to {_to}'.format(_from=start, _to=end), ui_log)
