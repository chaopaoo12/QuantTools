from datetime import datetime
from QUANTTOOLS.StockMarket.StockStrategySecond.train import train
from QUANTTOOLS.StockMarket.StockStrategySecond.running import predict
from QUANTTOOLS.StockMarket.StockStrategySecond.setting import working_dir
from QUANTAXIS.QAUtil.QADate_trade import QA_util_if_trade,QA_util_get_real_date,QA_util_get_last_day


def job111(trading_date):

    if datetime.strptime(trading_date, "%Y-%m-%d").weekday() == 5:
        if QA_util_if_trade(trading_date):
            pass
        else:
            trading_date = QA_util_get_real_date(trading_date)

        train(trading_date, working_dir=working_dir)
        predict(QA_util_get_last_day(trading_date))
    else:
        pass


