from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade
from QUANTTOOLS.QAStockETL.Check import (check_stock_60min, check_sinastock_60min,
                                         check_sinastock_adj,
                                         check_stock_techhour
                                         )
from QUANTTOOLS.QAStockETL.QASU import (QA_SU_save_stock_hour,QA_SU_save_single_stock_hour,
                                        QA_SU_save_stock_aklist)
from QUANTTOOLS.QAStockETL import (QA_SU_save_stock_technical_hour_day,
                                   QA_etl_stock_technical_hour)
import time

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        try:
            QA_SU_save_stock_aklist()
        except:
            pass

        res = check_stock_60min(mark_day)
        if res is None or len(res[1]) > 50:
            QA_SU_save_stock_hour()

        res = check_sinastock_60min(mark_day)
        while res is None or len(res[1]) > 1:
            for i in res[0] + res[1]:
                QA_SU_save_single_stock_hour(i)
            res = check_sinastock_60min(mark_day)

        res = check_sinastock_adj(mark_day)
        while res is None or len(res[1]) > 5:
            time.sleep(180)
            res = check_sinastock_adj(mark_day)

        res =check_stock_techhour(mark_day)
        if res is None or len(res[1]) > 50:
            QA_SU_save_stock_technical_hour_day(start_date = mark_day,  end_date = mark_day)
            res =check_stock_techhour(mark_day)

        QA_etl_stock_technical_hour(mark_day, mark_day)