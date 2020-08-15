from datetime import datetime
from QUANTTOOLS.StockMarket.StockStrategyThird.running import predict
from QUANTAXIS.QAUtil.QADate_trade import QA_util_if_trade,QA_util_get_real_date,QA_util_get_last_day


def daily_run(trading_date):

    if datetime.strptime(trading_date, "%Y-%m-%d").weekday() == 4:
        if QA_util_if_trade(trading_date):
            pass
        else:
            trading_date = QA_util_get_real_date(trading_date)

        predict(QA_util_get_last_day(trading_date))

    else:
        pass

    if QA_util_if_trade(trading_date):
        predict(trading_date)
    else:
        predict(QA_util_get_real_date(trading_date))


