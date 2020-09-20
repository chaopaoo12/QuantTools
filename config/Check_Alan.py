from QUANTTOOLS.QAStockETL.Check import (check_stock_day, check_stock_adj, check_tdx_financial, check_stock_60min)
from  QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_om_all

if __name__ == '__main__':
    mark_day = QA_util_today_str()
    if QA_util_if_trade(mark_day):

        target_subs = len(QA_fetch_stock_om_all().code.unique())
        res = check_stock_day(mark_day)
        if res is None or res >= target_subs/2:
            #send alan to change network
            pass
        else:
            pass

        res = check_stock_adj(mark_day)
        if res is None or res > target_subs/2:
            #alan
            pass
        else:
            pass

        res = check_tdx_financial(mark_day)
        if res is None or res > target_subs/2:
            #alan
            pass
        else:
            pass

        res = check_stock_60min(mark_day)
        if res is None or res > target_subs/2:
            #alan
            pass
        else:
            pass
