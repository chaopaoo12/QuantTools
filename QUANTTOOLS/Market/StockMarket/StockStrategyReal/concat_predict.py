
from QUANTTOOLS.Model.StockModel.StrategyXgboost import QAStockXGBoost
from QUANTTOOLS.Model.StockModel.StrategyXgboostReal import QAStockXGBoostReal
from QUANTTOOLS.Model.StockModel.StrategyXgboostHedge import QAStockXGBoostHedge
from QUANTTOOLS.Market.MarketTools import make_prediction,make_stockprediction,make_indexprediction

def concat_predict(trading_date, working_dir, model_name = 'stock_xg'):
    Stock = QAStockXGBoost()
    stock_info_temp = Stock.info
    target_pool,prediction,start,end,stock_info_temp['date'] = make_stockprediction(Stock, trading_date, model_name, working_dir)
    return(target_pool,prediction,start,end,stock_info_temp['date'])

def concat_predict_real(trading_date, working_dir, model_name = 'stock_xg_real'):
    Stock = QAStockXGBoostReal()
    stock_info_temp = Stock.info
    target_pool,prediction,start,end,stock_info_temp['date'] = make_stockprediction(Stock, trading_date, model_name, working_dir)
    return(target_pool,prediction,start,end,stock_info_temp['date'])

def concat_predict_hedge(trading_date, working_dir, model_name = 'hedge_xg'):
    Stock = QAStockXGBoostHedge()
    stock_info_temp = Stock.info
    target_pool,prediction,start,end,stock_info_temp['date'] = make_stockprediction(Stock, trading_date, model_name, working_dir)
    return(target_pool,prediction,start,end,stock_info_temp['date'])

def concat_predict_index(trading_date, working_dir, model_name = 'index_xg'):
    Stock = QAStockXGBoostHedge()
    stock_info_temp = Stock.info
    target_pool,prediction,start,end,stock_info_temp['date'] = make_indexprediction(Stock, trading_date, model_name, working_dir)
    return(target_pool,prediction,start,end,stock_info_temp['date'])