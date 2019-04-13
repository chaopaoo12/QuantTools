from QUANTTOOLS.QAStockETL.Crawly import get_stock_report_ths, get_stock_report_sina
from QUANTAXIS.QAUtil import QA_util_date_stamp

def QA_fetch_get_stock_report_ths(code):
    data = get_stock_report_ths(code)
    res = data.assign(report_date=data['report_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(res)

def QA_fetch_get_stock_report_sina(code, report_year):
    data = get_stock_report_sina(code, report_year)
    return(data)