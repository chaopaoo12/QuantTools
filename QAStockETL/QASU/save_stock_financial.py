
import pymongo
from QUANTAXIS.QAUtil.QATransform import QA_util_to_json_from_pandas
from QAStockETL.QAUtil.QAEtl import QA_util_etl_stock_financial
from QAStockETL.QAUtil.QASql import ASCENDING, DESCENDING
from QUANTAXIS.QAUtil import (DATABASE,QA_util_getBetweenQuarter, QA_util_get_next_day,
                              QA_util_get_real_date, QA_util_log_info,QA_util_add_months,
                              QA_util_to_json_from_pandas, trade_date_sse,QA_util_today_str,
                              QA_util_datetime_to_strdate,QA_util_get_trade_range)

def QA_SU_save_stock_fianacial_momgo(start_date=None,end_date=None):

    if start_date == None:
        if end_date == None:
            start_date = QA_util_today_str()
            end_date = start_date
        elif end_date is not None:
            start_date = '2003-01-01'
    elif start_date is not None:
        if end_date == None:
            start_date = QA_util_today_str()
            end_date = start_date
        elif end_date is not None:
            if end_date < start_date:
                print('end_date should large than start_date')

    deal_date_list = QA_util_get_trade_range(start_date, end_date)
    if deal_date_list is None:
        print('not a trading day')
    else:
        for deal_date in deal_date_list:
            data = QA_util_etl_stock_financial(deal_date)
            if data is not None:
                data = QA_util_to_json_from_pandas(data)
                print("got '{deal_date}' stock financial data.".format(deal_date=deal_date))
                col = DATABASE.stock_financial_analysis
                col.create_index(
                    [("CODE", ASCENDING), ("DATE", ASCENDING)], unique=True)
                try:
                    col.insert_many(data, ordered=False)
                    print("'{deal_date}' stock financial data has been stored imto mongodb.".format(deal_date=deal_date))
                except Exception as e:
                    if isinstance(e, MemoryError):
                        col.insert_many(data, ordered=True)
                    elif isinstance(e, pymongo.bulk.BulkWriteError):
                        pass
                pass