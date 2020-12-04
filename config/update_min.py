from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade
from QUANTTOOLS.QAStockETL.Check import (check_stock_15min, check_sinastock_15min,
                                         check_stock_half, check_sinastock_half,
                                         check_stock_adj,check_sinastock_adj,
                                         check_stock_alpha101half,check_sinastock_alpha101half,
                                         check_stock_alpha191half,check_sinastock_alpha191half,
                                         check_stock_tech15min
                                         )
from QUANTTOOLS.QAStockETL.QASU import (QA_SU_save_stock_15min,QA_SU_save_single_stock_15min,
                                        QA_SU_save_stock_half,QA_SU_save_stock_aklist,
                                        QA_SU_save_stock_xdxr,QA_SU_save_single_stock_xdxr)
from QUANTTOOLS.QAStockETL import (QA_SU_save_stock_alpha101half_day,QA_SU_save_stock_alpha191half_day,
                                   QA_SU_save_stock_technical_15min_day,
                                   QA_etl_stock_alpha101half_day,QA_etl_stock_alpha191half_day,
                                   QA_etl_stock_half,QA_etl_stock_technical_15min)
import time

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        QA_SU_save_stock_aklist()

        res = check_stock_15min(mark_day)
        if res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_stock_15min()

        res = check_sinastock_15min(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 1:
            for i in res[0] + res[1]:
                QA_SU_save_single_stock_15min(i)
            res = check_sinastock_15min(mark_day)

        res = check_sinastock_adj(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 0:
            time.sleep(180)
            res = check_sinastock_adj(mark_day)

        res =check_stock_tech15min(mark_day)
        if res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_stock_technical_15min_day(start_date = mark_day,  end_date = mark_day)
            res =check_stock_tech15min(mark_day)

        QA_etl_stock_technical_15min(mark_day, mark_day)