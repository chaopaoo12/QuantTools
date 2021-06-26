
from .setting import working_dir, percent, exceptions
from .concat_predict import concat_predict,concat_predict_real,concat_predict_hedge
from .running import predict_stock_summary
from QUANTTOOLS.Market.MarketTools import trading_base, load_data, trading_base2
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_last_day,QA_util_get_real_date,QA_util_if_trade,QA_util_log_info,QA_util_get_pre_trade_date
from QUANTTOOLS.Model.FactorTools.QuantMk import get_index_quant_hour,get_index_quant_data,get_quant_data
from QUANTTOOLS.Model.FactorTools.base_tools import find_stock

def trading(trading_date, func = concat_predict, model_name = 'stock_xg', file_name = 'prediction', percent = percent, account= 'name:client-1', working_dir = working_dir, exceptions = exceptions):

    data = get_index_quant_data(QA_util_get_pre_trade_date(trading_date,91),QA_util_get_last_day(trading_date),type='crawl', norm_type=None)
    r = data[['PASS_MARK']].groupby('code').describe()
    r.columns=['cnt','mean','std','min','p25','median','p75','max']
    rr = r.join(data.loc[QA_util_get_last_day(trading_date)][['SKDJ_K','SKDJ_TR','SKDJ_K_WK','SKDJ_TR_WK','SKDJ_K_HR','SKDJ_TR_HR']])
    rr['per'] = rr['p75'] / abs(rr['p25'])
    rr1 = rr[((rr.per >= 1.5)|(rr['std'] >=1.8))&(rr.p75 >= 1)].reset_index()
    QA_util_log_info(rr1[rr1.SKDJ_K <= 40])
    r_tar, prediction_tar, prediction = load_data(func, QA_util_get_last_day(trading_date), working_dir, model_name = 'stock_xg', file_name = 'prediction')

    r_tar = prediction_tar.loc[(QA_util_get_last_day(trading_date),find_stock(rr1[rr1.SKDJ_K <= 40].code.tolist())),]
    r_tar = r_tar[r_tar.y_pred==1].reset_index('date')

    #r_tar = prediction_tar[prediction_tar.RANK <= 20].loc[QA_util_get_last_day(trading_date)]
    #r_tar = prediction_tar[(prediction_tar.RANK <= 20)&(prediction_tar.TARGET5.isnull())].drop_duplicates(subset='NAME',keep='last').reset_index().sort_values(by=['date','RANK'],ascending=[False,True]).set_index('code')
    #r_tar = prediction_tar.loc[(slice(None),list(r_tar.index)),].loc[QA_util_get_last_day(trading_date)]
    per = prediction_tar[(prediction_tar.PASS_MARK.isnull())&(prediction_tar.O_PROB > 0.5)].shape[0]
    #data = get_quant_data(QA_util_get_pre_trade_date(trading_date,5),QA_util_get_last_day(trading_date),type='crawl', block=False, sub_block=False,norm_type=None)
    #pe_list = data[(data.ROE_RATE > 1)&(data.PE_RATE < 1)&(data.NETPROFIT_INRATE > 50)&(data.ROE_TTM >= 15)&(data.PE_TTM <= 30)&(data.STOCK_TYPE <= 2)]
    #pe_list = pe_list[(pe_list.SKDJ_K <= 40)&(pe_list.SKDJ_K_HR <= 50)].loc[QA_util_get_last_day(trading_date)][['SKDJ_K_WK','SKDJ_TR_WK','SKDJ_K','SKDJ_TR','SKDJ_K_HR','SKDJ_TR_HR','INDUSTRY','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']]
    pe_list = None
    if per >= 1:
        per = percent
    else:
        per = 0.6

    if pe_list is None:
        #target_pool,prediction,start,end,Model_Date = func(QA_util_get_last_day(trading_date), working_dir, code = list(r_tar.index), type = 'crawl', model_name = 'stock_mars_day')
        target_pool = r_tar.sort_values('SKDJ_K')
        QA_util_log_info(target_pool)
        #target_pool = target_pool.loc[QA_util_get_last_day(trading_date)].reindex(index=r_tar.index).dropna(how='all')
    else:
        #target_pool,prediction,start,end,Model_Date = func(QA_util_get_last_day(trading_date), working_dir, code = list(r_tar.index) + pe_list, type = 'crawl', model_name = 'stock_mars_day')
        target_pool = r_tar.append(pe_list).reset_index().drop_duplicates(subset=['code'],keep='first',inplace=False).set_index('code').sort_values('SKDJ_K')
        #target_pool = target_pool.loc[QA_util_get_last_day(trading_date)].reindex(index=pe_list.index).dropna(how='all')

    res = trading_base2(trading_date, target_pool, percent = per, account= account, title = model_name, exceptions = exceptions)
    return(res)

def trading_real(trading_date, func = concat_predict, model_name = 'stock_xg_real', file_name = 'prediction_real', percent = percent, account= 'name:client-1', working_dir = working_dir, exceptions = exceptions):

    r_tar, prediction_tar, prediction = load_data(func, trading_date, working_dir, model_name, file_name)

    res = trading_base(trading_date, r_tar, percent = percent, account= account, title = model_name, exceptions = exceptions)

    return(res)

def trading_hedge(trading_date, func = concat_predict, model_name = 'hedge_xg', file_name = 'prediction_hedge', percent = percent, account= 'name:client-1', working_dir = working_dir, exceptions = exceptions):

    r_tar, prediction_tar, prediction = load_data(func, trading_date, working_dir, model_name, file_name)

    res = trading_base(trading_date, r_tar, percent = percent, account= account, title = model_name, exceptions = exceptions)

    return(res)


def trading_summary(trading_date,percent = percent, account= 'name:client-1',exceptions = exceptions):

    r_tar, prediction_tar, prediction = load_data(predict_stock_summary, trading_date, working_dir, 'prediction_stock_summary')
    model_name = 'stock_xg'

    #buy_code = []
    #for i in list(r_tar.index):
    #    if QA_fetch_get_stock_realtm_ask(i) > 0:
    #        buy_code.append(i)

    #if buy_code is None or len(buy_code) == 0:
    #    r_tar, prediction_tar = load_data(concat_predict, trading_date, working_dir, 'stock_xg_real', 'prediction_real')
    #    model_name = 'stock_xg_real'
    #    percent = 0.2

    res = trading_base2(trading_date, r_tar, percent = percent, account= account, title = model_name, exceptions = exceptions)

    return(res)

if __name__ == '__main__':
    pass
