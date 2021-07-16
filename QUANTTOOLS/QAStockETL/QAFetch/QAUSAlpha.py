from QUANTTOOLS.QAStockETL.QAFetch.AlphaTools import (usstock_alpha, usstock_alpha101)
from QUANTAXIS.QAUtil import QA_util_date_stamp,QA_util_log_info
from QUANTTOOLS.QAStockETL.QAUtil import QA_util_get_trade_range,QA_util_if_trade

def QA_fetch_get_usstock_alpha(code, date):
    if QA_util_if_trade(date, 'us') == True:
        data = usstock_alpha(code, date).reset_index()
        if data is not None:
            names = list(data.columns)
            names[0] = 'code'
            data.columns = names
            data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
            return(data)
        else:
            QA_util_log_info(
                '##JOB Non Data UsStock Alpha191 for ============== {}'.format(date))
    else:
        QA_util_log_info(
            '##JOB Non Data UsStock Alpha191 for ============== {}'.format(date))

def QA_fetch_get_usstock_alpha101(code, start, end):
    deal_date_list = QA_util_get_trade_range(start, end, 'us')
    if deal_date_list is not None:
        data = usstock_alpha101(code, start, end)
        if data is not None:
            data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
            return(data)
        else:
            QA_util_log_info(
                '##JOB Non Data UsStock Alpha101 ============== from {_from} to {_to}'.format(_from=start, _to=end))
    else:
        QA_util_log_info(
            '##JOB Non Data UsStock Alpha101 ============== from {_from} to {_to}'.format(_from=start, _to=end))

if __name__ == '__main__':
    pass