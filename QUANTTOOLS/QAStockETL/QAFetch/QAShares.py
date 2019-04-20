from QUANTTOOLS.QAStockETL.Crawly import get_stock_shares_sina
from QUANTAXIS.QAUtil import QA_util_date_stamp

def QA_fetch_get_stock_shares_sina(code):
    data = get_stock_shares_sina(code)
    data = data.assign(send_date=data['send_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    data = data.assign(begin_date=data['begin_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    data = data.assign(crawl_date=data['crawl_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)