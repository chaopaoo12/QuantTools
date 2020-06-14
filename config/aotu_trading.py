from QUANTTOOLS.QAStockTradingDay.StockStrategySecond.trading import trading
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_last_day


if __name__ == '__main__':
    mark_day = QA_util_get_last_day(QA_util_today_str())
    trading(mark_day)