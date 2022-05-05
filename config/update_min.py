from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade
from QUANTTOOLS.QAStockETL.Check import (check_stock_1min, check_sinastock_1min, check_stock_vwap
                                         )
from QUANTTOOLS.QAStockETL.QASU import (QA_SU_save_stock_1min,QA_SU_save_single_stock_1min,
                                        QA_SU_save_stock_aklist)
from QUANTTOOLS.QAStockETL import (QA_SU_save_stock_vwap_day)
import time

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        QA_SU_save_stock_aklist()

        res = check_stock_1min(mark_day)
        if res is None or len(res[1]) > 500:
            QA_SU_save_stock_1min()

        res = check_sinastock_1min(mark_day)
        while res is None or len(res[1]) > 1:
            for i in res[0] + res[1]:
                QA_SU_save_single_stock_1min(i)
            res = check_sinastock_1min(mark_day)

        res = check_stock_vwap(mark_day)
        if res is None or len(res[1]) > 500:
            QA_SU_save_stock_vwap_day(start_date=mark_day, end_date=mark_day)
            res = check_stock_vwap(mark_day)
