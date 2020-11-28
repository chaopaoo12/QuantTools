from QUANTTOOLS.QAStockETL.Crawly import get_gold_day_sina, get_gold_min_sina
from QUANTAXIS.QAUtil import QA_util_date_stamp

def QA_fetch_get_gold_day(code, date):
    data = get_gold_day_sina(code, date)
    data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])),
                       code=code)
    return(data)

def QA_fetch_get_gold_min(code, type):
    data = get_gold_min_sina(code, type)
    data = data.assign(time_stamp=data['datetime'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])),
                       code=code)
    return(data)