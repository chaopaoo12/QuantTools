from QUANTTOOLS.QAStockETL.Crawly import read_data_ifzq
from QUANTAXIS.QAUtil import QA_util_date_stamp

def QA_fetch_get_realtime(code, type):
    if code[0:2] == '60':
        ex = 'sh'
    elif code[0:3] in ['000','002','300']:
        ex = 'sz'
    data = read_data_ifzq(code, ex, type)
    data = data.assign(datetime=data['datetime'].apply(lambda x: str(x)),
                       date=data['datetime'].apply(lambda x: str(x)[0:10]),
                       date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])))
    return(data)
