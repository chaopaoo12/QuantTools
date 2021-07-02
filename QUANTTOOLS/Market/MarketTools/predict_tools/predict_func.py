#coding :utf-8

import joblib
from QUANTTOOLS.QAStockETL.FuncTools.base_func import mkdir
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_index_name,QA_fetch_stock_name
from QUANTAXIS import QA_fetch_stock_info
from QUANTAXIS.QAUtil import (QA_util_log_info,QA_util_get_real_date)
from QUANTTOOLS.Message import send_actionnotice, send_email
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from dateutil.rrule import *
delta3 = timedelta(days=7)

def make_prediction(Model, trading_date, name, working_dir, code = None, type='crawl'):
    try:
        QA_util_log_info(
            '##JOB Now Load Model ==== {}'.format(str(trading_date)))

        Model = Model.load_model(name,working_dir = working_dir)
    except:
        send_email('错误报告', '无法正确加载模型,请检查', trading_date)
        send_actionnotice(name,
                          '错误报告:{}'.format(trading_date),
                          '无法正确加载模型,请检查',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )
    start = QA_util_get_real_date((datetime.strptime(trading_date, "%Y-%m-%d") + relativedelta(weekday=FR(-2))).strftime('%Y-%m-%d'))
    end = trading_date
    QA_util_log_info('##JOB Now Model Predict from {start} to {end} ==== {s}'.format(start = start, end = end, s = str(trading_date)))
    if code is None:
        target_pool, prediction = Model.model_predict(start, end, type=type)
    else:
        target_pool, prediction = Model.model_predict(start, end, code, type=type)
    return(Model, target_pool, prediction, start, end, Model.info['date'])

def make_stockprediction(Stock, trading_date, name, working_dir, code = None, index = 'date', type='crawl'):
    Model, target_pool, prediction, start, end, Model_date = make_prediction(Stock, trading_date, name, working_dir, code, type)
    QA_util_log_info('##JOB Now Add info to Predictions')

    NAME = QA_fetch_stock_name(prediction.reset_index()['code'].unique().tolist())

    if len([i for i in ['INDUSTRY', 'industry'] if i in list(prediction.columns)]) > 0:
        target_pool = target_pool.reset_index().set_index('code').join(NAME).reset_index().set_index([index,'code']).sort_index().rename(columns={'name':'NAME',})
        prediction = prediction.reset_index().set_index('code').join(NAME).reset_index().set_index([index,'code']).sort_index().rename(columns={'name':'NAME'})
    else:
        INDUSTRY = QA_fetch_stock_info(prediction.reset_index()['code'].unique().tolist())[['industry']]
        target_pool = target_pool.reset_index().set_index('code').join(NAME).join(INDUSTRY).reset_index().sort_values(by=[index,'RANK'],ascending=[False,True]).set_index([index,'code']).rename(columns={'name':'NAME','industry':'INDUSTRY'})
        prediction = prediction.reset_index().set_index('code').join(NAME).join(INDUSTRY).reset_index().sort_values(by=[index,'RANK'],ascending=[False,True]).set_index([index,'code']).rename(columns={'name':'NAME','industry':'INDUSTRY'})
    print(prediction)
    return(target_pool, prediction, start, end, Model_date)

def make_indexprediction(Index, trading_date, name, working_dir, code = None, index = 'date', type='crawl'):
    Model, target_pool, prediction, start, end, Model_date = make_prediction(Index, trading_date, name, working_dir, code, type)

    QA_util_log_info('##JOB Now Add info to Predictions')

    NAME = QA_fetch_index_name(prediction.reset_index()['code'].unique().tolist())

    target_pool = target_pool.reset_index().set_index('code').join(NAME).reset_index().sort_values(by=[index,'RANK'],ascending=[False,True]).set_index([index,'code']).rename(columns={'name':'NAME'})

    prediction = prediction.reset_index().set_index('code').join(NAME).reset_index().sort_values(by=[index,'RANK'],ascending=[False,True]).set_index([index,'code']).rename(columns={'name':'NAME'})

    return(target_pool, prediction, start, end, Model_date)

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
