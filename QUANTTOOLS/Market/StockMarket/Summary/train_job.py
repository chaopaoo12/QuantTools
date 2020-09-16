from datetime import datetime
from QUANTTOOLS.Market.StockMarket.Summary.train import train
from QUANTTOOLS.Market.StockMarket.Summary.running import predict
from QUANTTOOLS.Market.StockMarket.Summary.setting import working_dir
from QUANTAXIS.QAUtil.QADate_trade import QA_util_if_trade,QA_util_get_real_date,QA_util_get_last_day


def daily_train(trading_date):

    if datetime.strptime(trading_date, "%Y-%m-%d").weekday() == 5:
        if QA_util_if_trade(trading_date):
            pass
        else:
            trading_date = QA_util_get_real_date(trading_date)

        train(trading_date, working_dir=working_dir)
        predict(QA_util_get_last_day(trading_date))
    else:
        pass


