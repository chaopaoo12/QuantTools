from QUANTTOOLS.Market.MarketTools import load_data
from QUANTTOOLS.Market.StockMarket.StockStrategyReal.concat_predict import (concat_predict, concat_predict_hour)
from QUANTTOOLS.Market.StockMarket.StockStrategyReal.setting import working_dir
from QUANTTOOLS.QAStockETL.QAFetch.QAQuantFactor import QA_fetch_get_stock_quant_min,QA_fetch_get_stock_quant_hour


trading_date='2020-12-15'

def track(trading_date, hour):

    r_tar1, prediction_tar1, prediction1 = load_data(concat_predict, trading_date, working_dir, file_name='prediction')
    data = QA_fetch_get_stock_quant_min(prediction_tar1[(prediction_tar1.O_PROB>0.5)&(prediction_tar1.TARGET2.isna())].reset_index().code.unique().tolist(),'2020-12-04',trading_date)
    data[['KDJ_K_S','KDJ_D_S','KDJ_J_S']] = data[['KDJ_K','KDJ_D','KDJ_J']] .groupby('code').shift()
    return(data[data.CROSS_JC == 1][['MACD','DIF','DEA','KDJ_K_S','KDJ_D_S','KDJ_J_S','BOLL','UB','LB','TERNS']].loc[(trading_date+ ' '+hour,slice(None)),])

def track_hour(trading_date):

    r_tar1, prediction_tar1, prediction1 = load_data(concat_predict, trading_date, working_dir, file_name='prediction')
    data = QA_fetch_get_stock_quant_hour(prediction_tar1[(prediction_tar1.O_PROB>0.5)&(prediction_tar1.TARGET2.isna())].reset_index().code.unique().tolist(),'2020-12-04',trading_date)
    data[['KDJ_K_S','KDJ_D_S','KDJ_J_S']] = data[['KDJ_K','KDJ_D','KDJ_J']] .groupby('code').shift()
    res1 = concat_predict_hour(trading_date,working_dir, code= prediction_tar1[(prediction_tar1.O_PROB>0.5)&(prediction_tar1.TARGET2.isna())].reset_index().code.unique().tolist(),type = 'real', model_name = 'stock_mars_hour')
    return(data, res1)