from QUANTTOOLS.StockMarket.StockStrategySecond.Tracking import Tracking
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_last_day,QA_util_get_real_date,QA_util_if_trade


if __name__ == '__main__':
    if QA_util_get_last_day(QA_util_today_str()) == 'wrong date':
        mark_day = QA_util_get_real_date(QA_util_today_str())
    else:
        mark_day = QA_util_get_last_day(QA_util_today_str())

    if QA_util_if_trade(QA_util_today_str()):
        Tracking(mark_day)