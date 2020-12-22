#coding :utf-8

from QUANTTOOLS.Model.StockModel.StrategyXgboost import QAStockXGBoost
from QUANTTOOLS.Model.StockModel.StrategyXgboostHour import QAStockXGBoostHour
from QUANTTOOLS.Model.StockModel.StrategyXgboost15Min import QAStockXGBoost15Min
from QUANTTOOLS.Model.StockModel.StrategyXgboostReal import QAStockXGBoostReal
from QUANTTOOLS.Model.StockModel.StrategyXgboostHedge import QAStockXGBoostHedge
from QUANTTOOLS.Model.IndexModel.IndexXGboost import QAIndexXGBoost
from QUANTTOOLS.Model.IndexModel.IndexXGboostHour import QAIndexXGBoostHour
from QUANTTOOLS.Model.IndexModel.IndexXGboost15Min import QAIndexXGBoost15Min
from QUANTTOOLS.Market.MarketTools import make_prediction,make_stockprediction,make_indexprediction

def concat_predict(trading_date, working_dir, code = None, type = 'crawl', model_name = 'stock_mars_day'):
    Stock = QAStockXGBoost()
    target_pool,prediction,start,end,Model_Date = make_stockprediction(Stock, trading_date, model_name, working_dir, code, index='date', type=type)
    return(target_pool,prediction,start,end,Model_Date)

def concat_predict_hour(trading_date, working_dir, code = None, type = 'crawl', model_name = 'stock_mars_hour'):
    Stock = QAStockXGBoostHour()
    target_pool,prediction,start,end,Model_Date = make_stockprediction(Stock, trading_date, model_name, working_dir, code, index='datetime', type=type)
    return(target_pool,prediction,start,end,Model_Date)

def concat_predict_15min(trading_date, working_dir, code = None, type = 'crawl', model_name = 'stock_mars_min'):
    Stock = QAStockXGBoost15Min()
    target_pool,prediction,start,end,Model_Date = make_stockprediction(Stock, trading_date, model_name, working_dir, code, index='datetime', type=type)
    return(target_pool,prediction,start,end,Model_Date)

def concat_predict_index(trading_date, working_dir, type = 'crawl', model_name = 'index_mars_day'):
    Stock = QAIndexXGBoost()
    target_pool,prediction,start,end,Model_Date= make_indexprediction(Stock, trading_date, model_name, working_dir, index='date', type=type)
    return(target_pool,prediction,start,end,Model_Date)

def concat_predict_indexhour(trading_date, working_dir, type = 'crawl', model_name = 'index_mars_hour'):
    Stock = QAIndexXGBoostHour()
    target_pool,prediction,start,end,Model_Date= make_indexprediction(Stock, trading_date, model_name, working_dir, index='datetime', type=type)
    return(target_pool,prediction,start,end,Model_Date)

def concat_predict_index15min(trading_date, working_dir, type = 'crawl', model_name = 'index_mars_min'):
    Stock = QAIndexXGBoost15Min()
    target_pool,prediction,start,end,Model_Date= make_indexprediction(Stock, trading_date, model_name, working_dir, index='datetime', type=type)
    return(target_pool,prediction,start,end,Model_Date)

def concat_predict_real(trading_date, working_dir, type = 'model', model_name = 'stock_xg_real'):
    Stock = QAStockXGBoostReal()
    target_pool,prediction,start,end,Model_Date = make_stockprediction(Stock, trading_date, model_name, working_dir, type)
    return(target_pool,prediction,start,end,Model_Date)

def concat_predict_crawl(trading_date, working_dir, type = 'crawl', model_name = 'stock_xg_real'):
    Stock = QAStockXGBoostReal()
    target_pool,prediction,start,end,Model_Date = make_stockprediction(Stock, trading_date, model_name, working_dir, type)
    return(target_pool,prediction,start,end,Model_Date)

def concat_predict_hedge(trading_date, working_dir, type = 'model', model_name = 'hedge_xg'):
    Stock = QAStockXGBoostHedge()
    target_pool,prediction,start,end,Model_Date = make_stockprediction(Stock, trading_date, model_name, working_dir, type)
    return(target_pool,prediction,start,end,Model_Date)