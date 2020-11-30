from  QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade
from QUANTTOOLS.QAStockETL.Check import check_stock_code
from QUANTTOOLS.QAStockETL.QASU import QA_SU_save_stock_aklist
from QUANTAXIS.QASU.main import (QA_SU_save_stock_list,QA_SU_save_stock_info_tushare)
from QUANTTOOLS.Ananlysis.JOB.daily_job import aotu_report

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        aotu_report(mark_day)
        QA_SU_save_stock_list('tdx')
        #QA_SU_save_stock_info_tushare()
        QA_SU_save_stock_aklist()
        #QA_SU_save_stock_industryinfo()
        res = check_stock_code()