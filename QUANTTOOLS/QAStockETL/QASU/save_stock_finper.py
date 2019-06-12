
import pymongo
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_financial_percent
from QUANTTOOLS.QAStockETL.QAUtil import ASCENDING
from QUANTAXIS.QAUtil import (DATABASE, QA_util_to_json_from_pandas, QA_util_today_str,QA_util_code_tolist,
                              QA_util_get_trade_range)
import pandas as pd
from QUANTAXIS.QAFetch.QAQuery_Advance import (QA_fetch_stock_list_adv, QA_fetch_stock_block_adv,
                                               QA_fetch_stock_day_adv)


def QA_SU_save_stock_fianacial_percent(code, start_date=None,end_date=None):
    if code is None:
        code = list(QA_fetch_stock_list_adv()['code'])
    else:
        code = QA_util_code_tolist(code)

    if start_date is None:
        if end_date is None:
            start_date = QA_util_today_str()
            end_date = start_date
        elif end_date is not None:
            start_date = '2008-01-01'
    elif start_date is not None:
        if end_date == None:
            end_date = QA_util_today_str()
        elif end_date is not None:
            if end_date < start_date:
                print('end_date should large than start_date')

    col = DATABASE.stock_financial_percent
    col.create_index(
        [("code", ASCENDING), ("date_stamp", ASCENDING)], unique=True)

    data = QA_fetch_get_stock_financial_percent(code,start_date,end_date)
    if data is not None:
        data = data.drop_duplicates(
            (['code', 'date']))
        data = QA_util_to_json_from_pandas(data)
        print("got from '{from_}' to '{to_}' stock financial percent data.".format(from_ =start_date, to_ = end_date))

        try:
            col.insert_many(data, ordered=False)
            print("from '{from_}' to '{to_}' stock financial percent data has been stored imto mongodb.".format(from_ =start_date, to_ = end_date))
        except Exception as e:
            if isinstance(e, MemoryError):
                col.insert_many(data, ordered=True)
            elif isinstance(e, pymongo.bulk.BulkWriteError):
                pass
        pass