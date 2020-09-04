from QUANTTOOLS.QAStockETL.Crawly import get_stock_report_ths, get_stock_report_sina, read_stock_report_wy
from QUANTAXIS.QAUtil import QA_util_date_stamp

def QA_fetch_get_stock_report_ths(code):
    data = get_stock_report_ths(code)
    data = data.assign(report_date=data['report_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)

def QA_fetch_get_stock_report_sina(code, report_year):
    data = get_stock_report_sina(code, report_year).reset_index()
    data = data.assign(report_date=data['report_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    data = data.assign(crawl_date=data['crawl_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)

#def QA_fetch_get_stock_report_wy(code):
#    data = get_stock_report_wy(code)
#    data = data.assign(report_date=data['report_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
#    data = data.assign(crawl_date=data['crawl_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
#    return(data)

def QA_fetch_get_stock_report_wy(code):
    data = read_stock_report_wy(code)
    data = data.assign(report_date=data['report_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    data = data.assign(crawl_date=data['crawl_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)