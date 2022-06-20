from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade
from QUANTTOOLS.Market.MarketReport.JOB.daily_job import aotu_bond
from QUANTTOOLS.QAStockETL.Check import (check_stock_quant)
import time

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        res = check_stock_quant(mark_day)
        while res is None or len(res[1]) > 500:
            time.sleep(180)
            res = check_stock_quant(mark_day)
        aotu_bond(mark_day)
