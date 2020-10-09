from QUANTTOOLS.QAStockETL.Crawly import read_financial_report,read_stock_day
from QUANTAXIS.QAUtil import QA_util_date_stamp

def QA_fetch_get_usstock_report_xq(code):
    data = read_financial_report(code)
    data = data.assign(report_date=data['report_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)

def QA_fetch_get_stock_report_xq(code):
    data = read_financial_report(code)
    data = data.assign(report_date=data['report_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)

def QA_fetch_get_usstock_day_xq(code, start_date, end_date):
    data = read_stock_day(code, start_date, end_date).reset_index()
    data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)
