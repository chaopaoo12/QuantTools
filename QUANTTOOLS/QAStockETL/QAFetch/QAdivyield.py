
from QUANTTOOLS.QAStockETL.Crawly import get_stock_divyield
from QUANTAXIS.QAUtil import QA_util_date_stamp

def QA_fetch_get_stock_divyield(report_date):
    data = get_stock_divyield(report_date)
    data = data.assign(reg_date=data['reg_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    data = data.assign(report_date=data['report_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    data = data.assign(crawl_date=data['crawl_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    print(data.shape)
    return(data)
