from QUANTTOOLS.QAStockETL.QAUtil import alpha
from QUANTAXIS.QAUtil import QA_util_date_stamp,QA_util_if_trade

def QA_fetch_get_stock_alpha(code, date):
    if QA_util_if_trade(date) == True:
        data = alpha(code, date).reset_index()
        names = list(data.columns)
        names[0] = 'code'
        data.columns = names
        data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
        return(data)
    else:
        print("Not a Trading Day")
