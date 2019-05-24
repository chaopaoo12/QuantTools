
import pymongo
from QUANTTOOLS.QAStockETL.QAUtil import QA_util_etl_stock_quant
from QUANTTOOLS.QAStockETL.QAUtil import ASCENDING
from QUANTAXIS.QAUtil import (DATABASE, QA_util_to_json_from_pandas, QA_util_today_str,
                              QA_util_get_trade_range)
import pandas as pd

def QA_SU_save_stock_fianacial_momgo(start_date=None,end_date=None):
    if start_date is None:
        if end_date is None:
            start_date = QA_util_today_str()
            end_date = start_date
        elif end_date is not None:
            start_date = '2007-01-01'
    elif start_date is not None:
        if end_date == None:
            start_date = QA_util_today_str()
            end_date = start_date
        elif end_date is not None:
            if end_date < start_date:
                print('end_date should large than start_date')
    col = DATABASE.stock_financial_analysis
    col.create_index(
        [("CODE", ASCENDING), ("date_stamp", ASCENDING)], unique=True)

    deal_date_list = list(pd.date_range(start_date, end_date).map(lambda t:str(t.date())))
    if deal_date_list is None:
        print('not a trading day')
    else:
        for deal_date in deal_date_list:
            data = QA_util_etl_stock_quant(deal_date)
            if data is not None:
                data = data.drop_duplicates(
                    (['CODE', 'date']))
                data = QA_util_to_json_from_pandas(data)
                print("got '{deal_date}' stock financial data.".format(deal_date=deal_date))

                try:
                    col.insert_many(data, ordered=False)
                    print("'{deal_date}' stock financial data has been stored imto mongodb.".format(deal_date=deal_date))
                except Exception as e:
                    if isinstance(e, MemoryError):
                        col.insert_many(data, ordered=True)
                    elif isinstance(e, pymongo.bulk.BulkWriteError):
                        pass
                pass