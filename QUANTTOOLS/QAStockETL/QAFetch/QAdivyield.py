
from QUANTTOOLS.QAStockETL.Crawly import get_stock_divyield
from QUANTAXIS.QAUtil import QA_util_date_stamp

def QA_fetch_get_stock_divyield(report_date):
    data = get_stock_divyield(report_date)
    res = data.assign(date_stamp=data['dir_dcl_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(res)
