from QUANTAXIS.QASU.main import (QA_SU_save_stock_min)
from  QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade
from QUANTTOOLS.QAStockETL.Check import check_stock_60min, check_stock_alpha101half
from QUANTTOOLS.QAStockETL import (QA_SU_save_stock_alpha101half_day, QA_etl_stock_alpha101half_day)

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):

        res = check_stock_60min(mark_day)
        while res is None or res > 20:
            QA_SU_save_stock_min('tdx')

        res = check_stock_alpha101half(mark_day)
        while res is None or res > 20:
            QA_SU_save_stock_alpha101half_day(start_date = mark_day,  end_date = mark_day)

        QA_etl_stock_alpha101half_day(mark_day, mark_day)