
from QUANTTOOLS.QAStockETL.Crawly import get_financial_report_date
from QUANTAXIS.QAUtil import QA_util_date_stamp

def QA_fetch_get_financial_calendar(report_date):
    data = get_financial_report_date(report_date)
    res = data.assign(date_stamp=data['real_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(res)
