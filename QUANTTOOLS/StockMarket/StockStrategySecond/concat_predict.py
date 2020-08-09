import joblib
from QUANTTOOLS.FactorTools.base_func import mkdir
import QUANTTOOLS.QAStockTradingDay.StockModel.StrategyOne as Stock
import QUANTTOOLS.QAIndexTradingDay.IndexModel.IndexStrategyOne as Index
import pandas as pd
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_industry,QA_fetch_stock_name,QA_fetch_index_name
from QUANTTOOLS.FactorTools.base_tools import combine_model,combine_index
from QUANTTOOLS.message_func import send_email
from QUANTTOOLS.StockMarket.StockStrategySecond.setting import working_dir
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
        stock_model_temp, stock_info_temp = Stock.load_model('stock',working_dir = working_dir)
        index_model_temp, index_info_temp = Index.load_model('index',working_dir = working_dir)
        safe_model_temp, safe_info_temp = Index.load_model('safe',working_dir = working_dir)
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
    QA_util_log_info(
        '##JOB Now Index Model Predict ==== {}'.format(str(trading_date)), ui_log)
    #index_list,index_report,index_top_report = Index.check_model(index_model_temp, QA_util_get_last_day(trading_date),QA_util_get_last_day(trading_date),index_info_temp['cols'], 'INDEXT_TARGET5', 0.3)
    index_tar, index_b  = Index.model_predict(index_model_temp, start, end, index_info_temp['cols'], index_info_temp['fs'])
    QA_util_log_info(
        '##JOB Now Safe Model Predict ==== {}'.format(str(trading_date)), ui_log)
    #safe_list,safe_report,safe_top_report = Index.check_model(safe_model_temp, QA_util_get_last_day(trading_date),QA_util_get_last_day(trading_date),safe_info_temp['cols'], 'INDEXT_TARGET', 0.3)
    safe_tar, safe_b  = Index.model_predict(safe_model_temp, start, end, safe_info_temp['cols'], safe_info_temp['fs'])
    QA_util_log_info(
        '##JOB Now Stock Model Predict ==== {}'.format(str(trading_date)), ui_log)
    #stock_list,report,top_report = Stock.check_model(stock_model_temp, QA_util_get_last_day(trading_date),QA_util_get_last_day(trading_date),stock_info_temp['cols'], 0.42)
    stock_tar, stock_b  = Stock.model_predict(stock_model_temp, start, end, stock_info_temp['cols'], stock_info_temp['fs'])

    QA_util_log_info(
        '##JOB Now Combine Predictions ==== {}'.format(str(trading_date)), ui_log)

    tar_index = combine_index(index_b, safe_b, start, trading_date)
    tar = combine_model(index_b, stock_b, safe_b, start, trading_date)

    QA_util_log_info(
        '##JOB Now Add info to Predictions ==== {}'.format(str(trading_date)), ui_log)

    tar = tar.reset_index()
    tar['NAME'] = tar['code'].apply(lambda x:QA_fetch_stock_name(x))
    tar['INDUSTRY'] = tar['code'].apply(lambda x:QA_fetch_stock_industry(x))
    tar = tar.set_index(['date','code']).sort_index()

    tar_index = tar_index.reset_index()
    tar_index['NAME'] = tar_index['code'].apply(lambda x:QA_fetch_index_name(x))
    tar_index = tar_index.set_index(['date','code']).sort_index()

    index_tar = index_tar.reset_index()
    index_tar['NAME'] = index_tar['code'].apply(lambda x:QA_fetch_index_name(x))
    index_tar = index_tar.set_index(['date','code']).sort_index()

    safe_tar = safe_tar.reset_index()
    safe_tar['NAME'] = safe_tar['code'].apply(lambda x:QA_fetch_index_name(x))
    safe_tar = safe_tar.set_index(['date','code']).sort_index()

    stock_tar = stock_tar.reset_index()
    stock_tar['NAME'] = stock_tar['code'].apply(lambda x:QA_fetch_stock_name(x))
    stock_tar['INDUSTRY'] = stock_tar['code'].apply(lambda x:QA_fetch_stock_industry(x))
    stock_tar = stock_tar.set_index(['date','code']).sort_index()

    return(tar,tar_index,index_tar,safe_tar,stock_tar,start,end,stock_info_temp['date'])

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
