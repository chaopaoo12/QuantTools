from  QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade
from QUANTTOOLS.QAStockETL.Check import check_stock_alpha101real
from QUANTTOOLS.QAStockETL.QASU import QA_SU_save_stock_alpha101half_real

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        res = None
        while res is None or res > 20:
            QA_SU_save_stock_alpha101half_real()
            res = check_stock_alpha101real(mark_day)