import joblib
from QUANTTOOLS.QAStockETL.FuncTools.base_func import mkdir
import pandas as pd
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_industry,QA_fetch_stock_name
from QUANTAXIS.QAUtil import (QA_util_log_info)
from QUANTTOOLS.Message import send_actionnotice, send_email
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from dateutil.rrule import *
delta3 = timedelta(days=7)

def make_prediction(Stock, trading_date, name, working_dir, ui_log = None):
    try:
        QA_util_log_info(
            '##JOB Now Load Model ==== {}'.format(str(trading_date)), ui_log)

        Stock = Stock.load_model(name,working_dir = working_dir)
        stock_info_temp = Stock.info
    except:
        send_email('错误报告', '无法正确加载模型,请检查', trading_date)
        send_actionnotice(name,
                          '错误报告:{}'.format(trading_date),
                          '无法正确加载模型,请检查',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
    if datetime.strptime(trading_date, "%Y-%m-%d").weekday() == 4:
        start = (datetime.strptime(trading_date, "%Y-%m-%d") - delta3).strftime('%Y-%m-%d')
    else:
        start = (datetime.strptime(trading_date, "%Y-%m-%d") + relativedelta(weekday=FR(-1))).strftime('%Y-%m-%d')
    end = trading_date
    rng = pd.Series(pd.date_range(start, end, freq='D')).apply(lambda x: str(x)[0:10])
    QA_util_log_info('##JOB Now Stock Model Predict from {start} to {end} ==== {s}'.format(start = start, end = end, s = str(trading_date)), ui_log)
    target_pool, prediction  = Stock.model_predict(start, end)

    QA_util_log_info(
        '##JOB Now Add info to Predictions ==== {}'.format(str(trading_date)), ui_log)

    target_pool = target_pool.sort_index()

    prediction = prediction.sort_index()

    return(target_pool,prediction,start,end,stock_info_temp['date'])

def save_prediction(predict_info, name, working_dir):
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
