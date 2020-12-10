#coding :utf-8

from QUANTTOOLS.Market.MarketTools.predict_tools import save_prediction, prediction_report, Index_Report
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_real_date
import pandas as pd

def predict_base(trading_date, predict_func, model_name, file_name, top_num, percent, working_dir, exceptions):
    stock_tar, stock_b, start, end, model_date = predict_func(trading_date, model_name = model_name,  working_dir=working_dir)
    save_prediction({'date': trading_date, 'target_pool':stock_tar, 'prediction':stock_b}, file_name, working_dir)
    prediction_report(QA_util_get_real_date(trading_date), stock_tar, stock_b, model_date, top_num, exceptions, percent, account='name:client-1', ui_log = None)

def predict_index_base(trading_date, predict_func, model_name, file_name, top_num, working_dir):
    stock_tar, stock_b, start, end, model_date = predict_func(trading_date, model_name = model_name,  working_dir=working_dir)
    save_prediction({'date': trading_date, 'target_pool':stock_tar, 'prediction':stock_b}, file_name, working_dir)
    Index_Report(QA_util_get_real_date(trading_date), stock_tar, stock_b, model_date, top_num)

def predict_index_dev(trading_date, predict_func1, predict_func2, predict_func3, day_moel, hour_model, min_model, file_name, top_num, working_dir):
    stock_b = pd.DataFrame()
    if predict_func1 is not None:
        day_tar, day_b, start, end, model_date = predict_func1(trading_date, model_name = day_moel,  working_dir=working_dir)
        stock_b[['NAME','DAY_PROB','DAY_RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','PASS_MARK']] = day_b[['NAME','O_PROB','RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','PASS_MARK']]

    if predict_func2 is not None:
        hour_tar, hour_b, start, end, model_date = predict_func2(trading_date, model_name = hour_model,  working_dir=working_dir)
        rrr1 = hour_b.reset_index().set_index('datetime')
        rrr1 = rrr1[rrr1.index.hour == 15].reset_index()
        rrr1 = rrr1.assign(date=rrr1.datetime.apply(lambda x:str(x)[0:10])).set_index(['date','code'])
        stock_b[['HOUR_PROB']] = rrr1[['O_PROB']]

    if predict_func3 is not None:
        min_tar, min_b, start, end, model_date = predict_func3(trading_date, model_name = min_model,  working_dir=working_dir)
        rrr1 = min_b.reset_index().set_index('datetime')
        rrr1 = rrr1[rrr1.index.hour == 15].reset_index()
        rrr1 = rrr1.assign(date=rrr1.datetime.apply(lambda x:str(x)[0:10])).set_index(['date','code'])
        stock_b[['MIN_PROB']] = rrr1[['O_PROB']]

    stock_tar = stock_b[(stock_b.DAY_PROB > 0.5)]
    save_prediction({'date': trading_date, 'target_pool':stock_tar, 'prediction':stock_b}, file_name, working_dir)
    Index_Report(QA_util_get_real_date(trading_date), stock_tar, stock_b, model_date, top_num)


def predict_stock_dev(trading_date, predict_func1, predict_func2, predict_func3, day_moel, hour_model, min_model, file_name, top_num, percent, working_dir, exceptions):
    stock_b = pd.DataFrame()
    if predict_func1 is not None:
        day_tar, day_b, start, end, model_date = predict_func1(trading_date, model_name = day_moel,  working_dir=working_dir)
        stock_b[['NAME','DAY_PROB','DAY_RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']] = day_b[['NAME','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']]
    if predict_func2 is not None:
        hour_tar, hour_b, start, end, model_date = predict_func2(trading_date, model_name = hour_model,  working_dir=working_dir)
        rrr1 = hour_b.reset_index().set_index('datetime')
        rrr1 = rrr1[rrr1.index.hour == 15].reset_index()
        rrr1 = rrr1.assign(date=rrr1.datetime.apply(lambda x:str(x)[0:10])).set_index(['date','code'])
        stock_b[['HOUR_PROB']] = rrr1[['O_PROB']]

    if predict_func3 is not None:
        min_tar, min_b, start, end, model_date = predict_func3(trading_date, model_name = min_model,  working_dir=working_dir)
        rrr1 = min_b.reset_index().set_index('datetime')
        rrr1 = rrr1[rrr1.index.hour == 15].reset_index()
        rrr1 = rrr1.assign(date=rrr1.datetime.apply(lambda x:str(x)[0:10])).set_index(['date','code'])
        stock_b[['MIN_PROB']] = rrr1[['O_PROB']]

    stock_tar = stock_b[(stock_b.DAY_PROB > 0.5)]
    save_prediction({'date': trading_date, 'target_pool':stock_tar, 'prediction':stock_b}, file_name, working_dir)
    prediction_report(QA_util_get_real_date(trading_date), stock_tar, stock_b, model_date, top_num, exceptions, percent, account='name:client-1', ui_log = None)