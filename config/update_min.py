from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade
from QUANTAXIS.QASU.save_tdx import  QA_SU_save_single_stock_min
from QUANTTOOLS.QAStockETL.Check import check_stock_60min, check_sinastock_60min, check_sinastock_half, check_stock_alpha101half,check_stock_half,check_stock_alpha191half
from QUANTTOOLS.QAStockETL.QASU import QA_SU_save_stock_half,QA_SU_save_stock_min
from QUANTTOOLS.QAStockETL import (QA_SU_save_stock_alpha101half_day,QA_SU_save_stock_alpha191half_day, QA_etl_stock_alpha101half_day,QA_etl_stock_alpha191half_day)

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):

        res = check_stock_60min(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_stock_min()
            res = check_stock_60min(mark_day)

        res = check_stock_half(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_stock_half()
            res = check_stock_half(mark_day)

        res = check_stock_alpha101half(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_stock_alpha101half_day(start_date = mark_day,  end_date = mark_day)
            res = check_stock_alpha101half(mark_day)

        res = check_stock_alpha191half(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_stock_alpha191half_day(start_date = mark_day,  end_date = mark_day)
            res = check_stock_alpha191half(mark_day)

        QA_etl_stock_alpha101half_day(mark_day, mark_day)
        QA_etl_stock_alpha191half_day(mark_day, mark_day)