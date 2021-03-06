from  QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade
from QUANTTOOLS.QAStockETL.Check import check_stock_techreal,check_stock_real,check_sinastock_tech
from QUANTTOOLS.QAStockETL.QASU import QA_SU_save_stock_technical_index_half,QA_SU_save_stock_real

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):

        res = check_stock_real(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 0:
            QA_SU_save_stock_real()
            res = check_stock_real(mark_day)

        res = check_stock_techreal(mark_day)
        while res is None or (len(res[0]) + len(res[1])) > 20:
            QA_SU_save_stock_technical_index_half()
            res = check_stock_techreal(mark_day)

        res = check_sinastock_tech(mark_day)
        while res is None or len(res[1]) > 0:
            for i in res[0] + res[1]:
                QA_SU_save_stock_technical_index_half(code=i,start_date=mark_day, end_date = mark_day)
            res = check_sinastock_tech(mark_day)