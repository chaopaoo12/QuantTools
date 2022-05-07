from QUANTTOOLS.QAStockETL.Crawly import get_globalindex_day_sina
from QUANTAXIS.QAUtil import QA_util_date_stamp

def QA_fetch_get_globalindex_day(code):
    data = get_globalindex_day_sina(code)
    data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])),
                       code=code)
    return(data)