from datetime import datetime
from QUANTTOOLS.QAStockTradingDay.StockStrategySecond.train import train
from QUANTTOOLS.QAStockTradingDay.StockStrategySecond.running import predict
from QUANTTOOLS.QAStockTradingDay.StockStrategySecond.setting import working_dir
from QUANTAXIS.QAUtil.QADate_trade import QA_util_if_trade,QA_util_get_real_date,QA_util_get_last_day
from QUANTAXIS.QAUtil import QA_util_today_str

def job111(trading_date):

    if QA_util_get_last_day(QA_util_today_str()) == 'wrong date':
        mark_day = QA_util_get_real_date(QA_util_today_str())
    else:
        mark_day = QA_util_get_last_day(QA_util_today_str())

    if datetime.strptime(trading_date, "%Y-%m-%d").weekday() == 4:
        if QA_util_if_trade(trading_date):
            pass
        else:
            trading_date = QA_util_get_real_date(trading_date)

        predict(QA_util_get_last_day(trading_date))
        train(trading_date, working_dir=working_dir)
    else:
        pass

    if QA_util_if_trade(trading_date):
        predict(trading_date)
    else:
        predict(QA_util_get_real_date(trading_date))


