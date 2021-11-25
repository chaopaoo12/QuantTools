
from QUANTTOOLS.Market.StockMarket.StockStrategyReal.setting import exceptions
from QUANTTOOLS.Market.MarketTools import tracking_base

def Tracking(trading_date):
    tracking_base(trading_date, strategy_id = '机器学习1号',
                  account='name:client-1', exceptions=exceptions)


if __name__ == '__main__':
    pass