from QUANTTOOLS.QAStockETL.Crawly import get_btc_min_sina, get_btc_day_sina
from QUANTAXIS.QAUtil import QA_util_date_stamp
import pandas as pd

def QA_fetch_get_btc_day(code):
    data = get_btc_day_sina(code)
    data = data.assign(real_date=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)

def QA_fetch_get_btc_min(code, type):
    data = get_btc_min_sina(code, type, 1000)
    data = data.assign(real_date=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)