import joblib
from QUANTTOOLS.FactorTools.base_func import mkdir

import QUANTTOOLS.QAStockTradingDay.StockModel.StrategyXgboost as StockModelXGBosst
import QUANTTOOLS.QAStockTradingDay.StockModel.StrategyKeras as StockModelKeras
import QUANTTOOLS.QAIndexTradingDay.IndexModel.IndexKeras as IndexModelKeras
import QUANTTOOLS.QAIndexTradingDay.IndexModel.IndexXGboost as IndexModelXGBosst

import pandas as pd
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_industry,QA_fetch_stock_name,QA_fetch_index_name
from QUANTTOOLS.message_func import send_email
from QUANTTOOLS.StockMarket.StockStrategyThird.setting import working_dir
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.message_func.wechat import send_actionnotice
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from dateutil.rrule import *
delta3 = timedelta(days=7)

def concat_predict(trading_date, strategy_id='机器学习1号',  working_dir=working_dir, ui_log = None):

    try:
        QA_util_log_info(
            '##JOB Now Load Model ==== {}'.format(str(trading_date)), ui_log)
        stock_xgboost_temp, stock_xgboost_info = StockModelXGBosst.load_model('stock',working_dir = working_dir)
        stock_keras_temp, stock_keras_info = StockModelKeras.load_model('stock',working_dir = working_dir)
        index_xgboost_temp, index_xgboost_info = IndexModelXGBosst.load_model('index',working_dir = working_dir)
        index_keras_temp, index_keras_info = IndexModelKeras.load_model('index',working_dir = working_dir)
    except:
        send_email('错误报告', '无法正确加载模型,请检查', trading_date)
        send_actionnotice(strategy_id,
                          '错误报告:{}'.format(trading_date),
                          '无法正确加载模型,请检查',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
    start = (datetime.strptime(trading_date, "%Y-%m-%d") + relativedelta(weekday=FR(-1))).strftime('%Y-%m-%d')
    end = trading_date
    rng = pd.Series(pd.date_range(start, end, freq='D')).apply(lambda x: str(x)[0:10])

    QA_util_log_info('##JOB Now Index Model Predict ==== {}'.format(str(trading_date)), ui_log)
    index_xgboost_tar, index_xgboost_b  = IndexModelXGBosst.model_predict(index_xgboost_temp, start, end, index_xgboost_info['cols'], index_xgboost_info['thresh'])
    index_keras_tar, index_keras_b  = IndexModelKeras.model_predict(index_keras_temp, start, end, index_keras_info['cols'], index_keras_info['thresh'])

    QA_util_log_info('##JOB Now Stock Model Predict ==== {}'.format(str(trading_date)), ui_log)
    stock_xgboost_tar, stock_xgboost_b  = StockModelXGBosst.model_predict(stock_xgboost_temp, start, end, stock_xgboost_info['cols'], stock_xgboost_info['thresh'])
    stock_keras_tar, stock_keras_b  = StockModelKeras.model_predict(stock_keras_temp, start, end, stock_keras_info['cols'], stock_keras_info['thresh'])

    QA_util_log_info('##JOB Now Combine Predictions ==== {}'.format(str(trading_date)), ui_log)

    ix_tar = index_xgboost_tar[index_xgboost_tar.RANK <= 5]
    ik_tar = index_keras_tar[index_keras_tar.RANK <= 5]
    sx_tar = stock_xgboost_tar[stock_xgboost_tar.RANK <= 5]
    sk_tar = stock_keras_tar[stock_keras_tar.RANK <= 5]

    QA_util_log_info(
        '##JOB Now Add info to Predictions ==== {}'.format(str(trading_date)), ui_log)

    stock_xgboost_tar = stock_xgboost_tar.reset_index()
    stock_xgboost_tar['NAME'] = stock_xgboost_tar['code'].apply(lambda x:QA_fetch_stock_name(x))
    stock_xgboost_tar['INDUSTRY'] = stock_xgboost_tar['code'].apply(lambda x:QA_fetch_stock_industry(x))
    stock_xgboost_tar = stock_xgboost_tar.set_index(['date','code']).sort_index()

    stock_keras_tar = stock_keras_tar.reset_index()
    stock_keras_tar['NAME'] = stock_keras_tar['code'].apply(lambda x:QA_fetch_stock_name(x))
    stock_keras_tar['INDUSTRY'] = stock_keras_tar['code'].apply(lambda x:QA_fetch_stock_industry(x))
    stock_keras_tar = stock_keras_tar.set_index(['date','code']).sort_index()

    index_xgboost_tar = index_xgboost_tar.reset_index()
    index_xgboost_tar['NAME'] = index_xgboost_tar['code'].apply(lambda x:QA_fetch_index_name(x))
    index_xgboost_tar = index_xgboost_tar.set_index(['date','code']).sort_index()

    index_keras_tar = index_keras_tar.reset_index()
    index_keras_tar['NAME'] = index_keras_tar['code'].apply(lambda x:QA_fetch_index_name(x))
    index_keras_tar = index_keras_tar.set_index(['date','code']).sort_index()

    return(stock_xgboost_tar,stock_keras_tar,index_xgboost_tar,index_keras_tar,start,end,stock_xgboost_info['date'])

def save_prediction(predict_info, name, working_dir = working_dir):
    if mkdir(working_dir):
        try:
            joblib.dump(predict_info, working_dir+"\\{name}.joblib.dat".format(name=name))
            print("dump success")
            return(True)
        except:
            print("dump fail")
            return(False)

def load_prediction(name, working_dir= 'D:\\model\\current'):
    res = joblib.load(working_dir+"\\{name}.joblib.dat".format(name=name))
    return(res)

def check_prediction(prediction, date):
    if prediction['date'] == date:
        pass
    else:
        raise Exception('预测需更新')
