from datetime import datetime
from QUANTTOOLS.StockMarket.StockStrategyThird.running import predict
from QUANTAXIS.QAUtil.QADate_trade import QA_util_if_trade,QA_util_get_real_date,QA_util_get_last_day


def daily_run(trading_date):

    if QA_util_if_trade(trading_date):
        predict(trading_date)
    elif QA_util_if_trade((datetime.datetime.strptime(trading_date,'%Y-%m-%d')+datetime.timedelta(days=1)).strftime("%Y-%m-%d")):
        predict(QA_util_get_real_date(trading_date))
    else:
        pass


