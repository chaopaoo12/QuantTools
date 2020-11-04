from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade
from QUANTTOOLS.QAStockETL.Check import (check_stock_60min, check_sinastock_60min,
                                         check_stock_half, check_sinastock_half,
                                         check_stock_adj,check_sinastock_adj,
                                         check_stock_alpha101half,check_sinastock_alpha101half,
                                         check_stock_alpha191half,check_sinastock_alpha191half
                                         )
from QUANTTOOLS.QAStockETL.QASU import (QA_SU_save_stock_min,QA_SU_save_single_stock_min,
                                        QA_SU_save_stock_half,QA_SU_save_stock_aklist,
                                        QA_SU_save_stock_xdxr,QA_SU_save_single_stock_xdxr)
from QUANTTOOLS.QAStockETL import (QA_SU_save_stock_alpha101half_day,QA_SU_save_stock_alpha191half_day,
                                   QA_etl_stock_alpha101half_day,QA_etl_stock_alpha191half_day,
                                   QA_etl_stock_half)
import time

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        QA_SU_save_stock_aklist()

        res = check_stock_60min(mark_day)
        if res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_stock_min()

        res = check_sinastock_60min(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 0:
            for i in res[0] + res[1]:
                QA_SU_save_single_stock_min(i)
            res = check_sinastock_60min(mark_day)

        res = check_stock_half(mark_day)
        if res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_stock_half()

        res = check_sinastock_half(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 0:
            QA_SU_save_stock_half()
            res = check_sinastock_half(mark_day)

        res = check_sinastock_adj(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 0:
            time.sleep(180)
            res = check_sinastock_adj(mark_day)

        res = check_stock_alpha101half(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_stock_alpha101half_day(start_date = mark_day,  end_date = mark_day)
            res = check_stock_alpha101half(mark_day)

        res = check_sinastock_alpha101half(mark_day)
        if res is None or (len(res[0]) + len(res[1])) > 0:
            for i in res[0] + res[1]:
                QA_SU_save_stock_alpha101half_day(code=i, start_date = mark_day,  end_date = mark_day)
            check_sinastock_alpha101half(mark_day)

        res = check_stock_alpha191half(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_stock_alpha191half_day(start_date = mark_day,  end_date = mark_day)
            res = check_stock_alpha191half(mark_day)
        check_sinastock_alpha191half(mark_day)

        QA_etl_stock_half(mark_day=mark_day)
        QA_etl_stock_alpha101half_day(mark_day, mark_day)
        QA_etl_stock_alpha191half_day(mark_day, mark_day)