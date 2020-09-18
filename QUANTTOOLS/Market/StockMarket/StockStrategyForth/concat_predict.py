import joblib
from QUANTTOOLS.QAStockETL.FuncTools.base_func import mkdir
from QUANTTOOLS.Model.StockModel.StrategyXgboost import QAStockXGBoost
import pandas as pd
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_industry,QA_fetch_stock_name
from .setting import working_dir
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.Message import send_actionnotice, send_email
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from dateutil.rrule import *
delta3 = timedelta(days=7)


def concat_predict(trading_date, strategy_id='机器学习1号',  working_dir=working_dir, ui_log = None):
    Stock = QAStockXGBoost()
    try:
        QA_util_log_info(
            '##JOB Now Load Model ==== {}'.format(str(trading_date)), ui_log)

        Stock = Stock.load_model('stock_xg',working_dir = working_dir)
        stock_info_temp = Stock.info
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
    QA_util_log_info('##JOB Now Stock Model Predict from {start} to {end} ==== {s}'.format(start = start, end = end, s = str(trading_date)), ui_log)
    stock_tar, stock_b  = Stock.model_predict(start, end)

    QA_util_log_info('##JOB Now Combine Predictions ==== {}'.format(str(trading_date)), ui_log)

    tar = stock_tar[stock_tar.RANK <= 5]

    QA_util_log_info(
        '##JOB Now Add info to Predictions ==== {}'.format(str(trading_date)), ui_log)

    tar = tar.reset_index()
    tar['NAME'] = tar['code'].apply(lambda x:QA_fetch_stock_name(x))
    tar['INDUSTRY'] = tar['code'].apply(lambda x:QA_fetch_stock_industry(x))
    tar = tar.set_index(['date','code']).sort_index()

    stock_tar = stock_tar.reset_index()
    stock_tar['NAME'] = stock_tar['code'].apply(lambda x:QA_fetch_stock_name(x))
    stock_tar['INDUSTRY'] = stock_tar['code'].apply(lambda x:QA_fetch_stock_industry(x))
    stock_tar = stock_tar.set_index(['date','code']).sort_index()

    return(tar,stock_tar,start,end,stock_info_temp['date'])

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
