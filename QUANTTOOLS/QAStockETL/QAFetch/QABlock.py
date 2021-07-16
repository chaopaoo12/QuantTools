from QUANTTOOLS.QAStockETL.Crawly import read_stock_day
from QUANTAXIS.QAUtil import QA_util_date_stamp
from QUANTTOOLS.QAStockETL.QAData.settting import bk, SW

def QA_fetch_get_block_day_xq(code, start_date, end_date):
    try:
        name = bk[code]
    except:
        name = SW[code]
    data = read_stock_day(code, start_date, end_date)
    data = data.assign(date_stamp=data['date'].apply(lambda x: QA_util_date_stamp(str(x)[0:10])),
                       name = name)
    return(data)

if __name__ == '__main__':
    pass