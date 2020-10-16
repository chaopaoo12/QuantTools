from  QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade
from QUANTTOOLS.QAStockETL.Check import check_stock_alpha191real, check_stock_real, check_stock_code
from QUANTTOOLS.QAStockETL.QASU import QA_SU_save_stock_alpha191half_real,QA_SU_save_stock_real,QA_SU_save_stock_aklist
from QUANTAXIS.QASU.main import (QA_SU_save_stock_block,QA_SU_save_stock_list,QA_SU_save_stock_info_tushare)

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):

        res = check_stock_code()
        while len(res) > 0:
            QA_SU_save_stock_list('tdx')
            QA_SU_save_stock_info_tushare()
            QA_SU_save_stock_aklist()
            #QA_SU_save_stock_industryinfo()
            res = check_stock_code()

        res = check_stock_real(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 0:
            QA_SU_save_stock_real()
            res = check_stock_real(mark_day)

        res = check_stock_alpha191real(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_stock_alpha191half_real()
            res = check_stock_alpha191real(mark_day)