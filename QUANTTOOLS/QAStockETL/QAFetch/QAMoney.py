from QUANTTOOLS.QAStockETL.Crawly import get_money_day_sina, get_money_min_sina, get_diniw_min_sina
from QUANTAXIS.QAUtil import QA_util_date_stamp

def QA_fetch_get_money_day(code, date):
    data = get_money_day_sina(code, date)
    data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])),
                       code=code)
    return(data)

def QA_fetch_get_money_min(code, type, lens=1000):
    data = get_money_min_sina(code, type, lens)
    data = data.assign(time_stamp=data['datetime'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])),
                       code=code,
                       type=type)
    return(data)

def QA_fetch_get_diniw_min(type, lens=1000):
    data = get_diniw_min_sina(type, lens)
    data = data.assign(time_stamp=data['datetime'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])),
                       code='DINIW',
                       type=type)
    return(data)