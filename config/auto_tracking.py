from QUANTTOOLS.Market.StockMarket.StockStrategyReal import trading_new, tracking_new
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_last_day,QA_util_get_real_date,QA_util_if_trade


if __name__ == '__main__':

    mark_day = QA_util_today_str()
    if QA_util_if_trade(mark_day):
        tracking_new(mark_day)