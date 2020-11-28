from QUANTTOOLS.QAStockETL.Crawly import get_btc_min_sina, get_btc_day_sina
from QUANTAXIS.QAUtil import QA_util_date_stamp

def QA_fetch_get_btc_day(code):
    data = get_btc_day_sina(code)
    data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])),
                       code=code)
    return(data)

def QA_fetch_get_btc_min(code, type=15):
    data = get_btc_min_sina(code, type, 1000)
    data = data.assign(time_stamp=data['datetime'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])),
                       code=code)
    return(data)