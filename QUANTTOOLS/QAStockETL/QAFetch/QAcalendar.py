
from QUANTTOOLS.QAStockETL.Crawly import get_financial_report_date
from QUANTAXIS.QAUtil import QA_util_date_stamp

def QA_fetch_get_financial_calendar(report_date):
    data = get_financial_report_date(report_date)
    data = data.assign(real_date=data['real_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    data = data.assign(report_date=data['report_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    data = data.assign(crawl_date=data['crawl_date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)

if __name__ == '__main__':
    pass