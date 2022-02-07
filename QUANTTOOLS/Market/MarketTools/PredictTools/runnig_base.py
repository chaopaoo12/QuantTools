#coding :utf-8

from QUANTTOOLS.Market.MarketTools.PredictTools import save_prediction, prediction_report, Index_Report, base_report
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_real_date
import pandas as pd

def predict_base(trading_date, predict_func, model_name, file_name, top_num, percent, working_dir, exceptions):
    stock_tar, stock_b, start, end, model_date, model_name, target = predict_func(trading_date, model_name = model_name,  working_dir=working_dir)
    save_prediction({'date': trading_date, 'target_pool':stock_tar, 'prediction':stock_b}, file_name, working_dir)
    prediction_report(QA_util_get_real_date(trading_date), stock_tar, stock_b, model_date, model_name, target, top_num, exceptions, percent, account='name:client-1', ui_log = None)

def predict_index_base(trading_date, predict_func, model_name, file_name, top_num, working_dir):
    stock_tar, stock_b, start, end, model_date = predict_func(trading_date, model_name = model_name,  working_dir=working_dir)
    save_prediction({'date': trading_date, 'target_pool':stock_tar, 'prediction':stock_b}, file_name, working_dir)
    Index_Report(QA_util_get_real_date(trading_date), stock_tar, stock_b, model_date, top_num)

def predict_index_dev(trading_date, predict_func1, predict_func2, predict_func3, day_moel, hour_model, min_model, file_name, top_num, working_dir):
    stock_b = pd.DataFrame()
    res = dict()
    if predict_func1 is not None:
        day_tar, day_b, start, end, model_date = predict_func1(trading_date, model_name = day_moel,  working_dir=working_dir)
        stock_b[['NAME','SKDJ_TR','SKDJ_K','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_CROSS2','SKDJ_CROSS1','CROSS_JC','RSI3','RSI2','RSI3_C','RSI2_C','DAY_PROB','DAY_RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','PASS_MARK']] = day_b[['NAME','SKDJ_TR','SKDJ_K','SKDJ_TR_WK','SKDJ_K_WK','SKDJ_CROSS2','SKDJ_CROSS1','CROSS_JC','RSI3','RSI2','RSI3_C','RSI2_C','O_PROB','RANK','INDEX_TARGET','INDEX_TARGET3','INDEX_TARGET4','INDEX_TARGET5','PASS_MARK']]
        res['date'] = trading_date

    if predict_func2 is not None:
        hour_tar, hour_b, start, end, model_date = predict_func2(trading_date, model_name = hour_model,  working_dir=working_dir)
        rrr1 = hour_b.reset_index().set_index('datetime')
        rrr1 = rrr1[rrr1.index.hour == 15].reset_index()
        rrr1 = rrr1.assign(date=rrr1.datetime.apply(lambda x:str(x)[0:10])).set_index(['date','code'])
        stock_b[['SKDJ_TR_HR','SKDJ_K_HR','HOUR_PROB']] = rrr1[['SKDJ_TR_HR','SKDJ_K_HR','O_PROB']]
        res['hour_prediction'] = hour_b

    if predict_func3 is not None:
        min_tar, min_b, start, end, model_date = predict_func3(trading_date, model_name = min_model,  working_dir=working_dir)
        rrr1 = min_b.reset_index().set_index('datetime')
        rrr1 = rrr1[rrr1.index.hour == 15].reset_index()
        rrr1 = rrr1.assign(date=rrr1.datetime.apply(lambda x:str(x)[0:10])).set_index(['date','code'])
        stock_b[['SKDJ_TR_15M','SKDJ_K_15M','MIN_PROB']] = rrr1[['SKDJ_TR_15M','SKDJ_K_15M','O_PROB']]
        res['min_prediction'] = min_b

    stock_tar = stock_b[(stock_b.DAY_PROB > 0.5)]
    res['target_pool'] = stock_tar
    res['prediction'] = stock_b

    save_prediction(res, file_name, working_dir)
    Index_Report(QA_util_get_real_date(trading_date), res['prediction'], res['hour_prediction'], model_date, )


def predict_stock_dev(trading_date, xg_predict_func, predict_func1, predict_func2, predict_func3, xg_model, day_moel, hour_model, min_model, file_name, top_num, percent, working_dir, exceptions):
    stock_b = pd.DataFrame()
    res = dict()
    if xg_predict_func is not None:
        xg_tar, xg_b, start, end, model_date = predict_func1(trading_date, model_name = xg_model,  working_dir=working_dir)
        stock_b[['NAME','INDUSTRY','SKDJ_K','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']] = xg_b[['NAME','INDUSTRY','SKDJ_K','O_PROB','RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK']]
        res['date'] = trading_date
        res['XG_prediction'] = xg_b

    if predict_func1 is not None:
        day_tar, day_b, start, end, model_date = predict_func1(trading_date, model_name = day_moel,  working_dir=working_dir)
        stock_b[['SKDJ_TR','SKDJ_K','DAY_PROB','DAY_RANK']] = day_b[['SKDJ_TR','SKDJ_K','O_PROB','RANK']]
        res['day_prediction'] = day_b

    if predict_func2 is not None:
        hour_tar, hour_b, start, end, model_date = predict_func2(trading_date, model_name = hour_model,  working_dir=working_dir)
        rrr1 = hour_b.reset_index().set_index('datetime')
        rrr1 = rrr1[rrr1.index.hour == 15].reset_index()
        rrr1 = rrr1.assign(date=rrr1.datetime.apply(lambda x:str(x)[0:10])).set_index(['date','code'])
        stock_b[['SKDJ_TR_HR','SKDJ_K_HR','HOUR_PROB']] = rrr1[['SKDJ_TR_HR','SKDJ_K_HR','O_PROB']]
        res['hour_prediction'] = hour_b

    if predict_func3 is not None:
        min_tar, min_b, start, end, model_date = predict_func3(trading_date, model_name = min_model,  working_dir=working_dir)
        rrr1 = min_b.reset_index().set_index('datetime')
        rrr1 = rrr1[rrr1.index.hour == 15].reset_index()
        rrr1 = rrr1.assign(date=rrr1.datetime.apply(lambda x:str(x)[0:10])).set_index(['date','code'])
        stock_b[['SKDJ_TR_15M','SKDJ_K_15M','MIN_PROB']] = rrr1[['SKDJ_TR_15M','SKDJ_K_15M','O_PROB']]
        res['min_prediction'] = min_b

    stock_tar = stock_b[(stock_b.O_PROB > 0.5)]
    res['target_pool'] = stock_tar
    res['prediction'] = stock_b

    save_prediction(res, file_name, working_dir)
    prediction_report(QA_util_get_real_date(trading_date), stock_tar, stock_b, model_date, top_num, exceptions, percent,
                      name_list = ['NAME','INDUSTRY','SKDJ_TR'],
                      value_ist = ['SKDJ_K','SKDJ_K_HR','O_PROB','RANK','DAY_PROB','DAY_RANK','TARGET','TARGET3','TARGET4','TARGET5','PASS_MARK','UB','LB','WIDTH','UB_HR','LB_HR','WIDTH_HR','RSI1','RSI3'],
                      sort_mark ='DAY_RANK',
                      selec_list=['NAME','INDUSTRY','SKDJ_TR','SKDJ_K','SKDJ_K_HR','O_PROB','DAY_PROB','RANK','DAY_RANK','UB','LB','WIDTH','UB_HR','LB_HR','WIDTH_HR'],
                      account='name:client-1', ui_log = None)

if __name__ == '__main__':
    pass