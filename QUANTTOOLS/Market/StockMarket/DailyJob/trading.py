
from QUANTTOOLS.Market.StockMarket.StockStrategyReal.setting import working_dir, percent, exceptions, strategy_id
from QUANTTOOLS.Market.StockMarket.StockStrategyReal.concat_predict import concat_predict, concat_predict_neut
from QUANTTOOLS.Market.MarketTools import trading_base, load_data, trading_base2, StrategyRobotBase, StrategyBase
from QUANTAXIS.QAUtil import QA_util_get_last_day,QA_util_get_real_date, QA_util_get_pre_trade_date
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_all,QA_fetch_get_stock_llv
from QUANTTOOLS.Market.StockMarket.StockStrategyReal.StrategyOne import signal, balance

def trading(trading_date, func = concat_predict_neut, model_name = 'stock_xg', file_name = 'prediction', percent = percent, account= 'name:client-1', working_dir = working_dir, exceptions = exceptions):

    r_tar, prediction_tar, prediction = load_data(func, QA_util_get_last_day(trading_date), working_dir, model_name = 'stock_xg_base', file_name = 'prediction_stock_xg_base')
    #r_tar1, prediction_tar1, prediction1 = load_data(concat_predict_index, QA_util_get_last_day(trading_date), working_dir, 'index_xg', 'prediction_index_summary')

    #try:
    #    res = prediction_tar1[(prediction_tar1.O_PROB>=0.5)&(prediction_tar1.INDEX_TARGET3.isnull())].reset_index().code.tolist()
    #except:
    #    res = prediction_tar1[(prediction_tar1.DAY_PROB>=0.5)&(prediction_tar1.INDEX_TARGET3.isnull())].reset_index().code.tolist()

    #lll = prediction_tar1.loc[QA_util_get_last_day(trading_date)].loc[res]

    #rrr = prediction_tar.loc[(slice(None),find_stock(list(lll[(lll.RSI3>lll.RSI2)].index))),]
    #rrr = rrr[(rrr.y_pred==1)&(rrr.TARGET5.isnull())].sort_values('RANK')

    #data = get_index_quant_data(QA_util_get_pre_trade_date(trading_date,91),QA_util_get_last_day(trading_date),type='crawl', norm_type=None)

    #r = data[['PASS_MARK']].groupby('code').describe()
    #r.columns=['cnt','mean','std','min','p25','median','p75','max']
    #rr = r.join(data.loc[QA_util_get_last_day(trading_date)][['SKDJ_K','SKDJ_TR','SKDJ_K_WK','SKDJ_TR_WK','SKDJ_K_HR','SKDJ_TR_HR']])
    #rr['per'] = rr['p75'] / abs(rr['p25'])
    #rr1 = rr[((rr.per >= 1.5)|(rr['std'] >=1.8))&(rr.p75 >= 1)].reset_index()
    #QA_util_log_info(rr1[rr1.SKDJ_K <= 40])
    #r_tar = prediction_tar.loc[(QA_util_get_last_day(trading_date),find_stock(rr1[(rr1.SKDJ_K <= 40)|(rr1.SKDJ_K_HR <= 40)].code.tolist())),]
    #r_tar = r_tar[(r_tar.y_pred==1)&(r_tar.TARGET3.isnull())]

    data = get_quant_data(QA_util_get_pre_trade_date(trading_date,6),QA_util_get_last_day(trading_date),code=QA_fetch_stock_all().code.tolist(),type='crawl', block=False, sub_block=False,norm_type=None,ST=False)
    #pe_list = data[(data.NETPROFIT_INRATE > 50)&(data.ROE_TTM >= 10)&(data.PE_TTM <= 100)&(data.SHORT10.abs() < 0.01)&(data.SHORT20 > -0.01)&(data.SHORT20 < 0)&(data.MA60_C > 0)&(data.ATRR > 0.02)]
    #pe_list = pe_list[(pe_list.TARGET.isnull())].sort_values('RANK')
    #pe_list = prediction_tar.loc[prediction_tar.index.intersection(pe_list.index)]

    indicator = QA_fetch_get_stock_llv(prediction_tar.reset_index().code.tolist(),QA_util_get_pre_trade_date(trading_date,6),QA_util_get_last_day(trading_date),'day').set_index(['date','code'])

    prediction_tar = prediction_tar.join(data[['SHORT10','SHORT20','MA60_C']])
    res = prediction_tar.join(indicator)
    try:
        r_tar = res[(res.LLS > 0)&(res.LLL > 0)&(res.RANK <= 20)].loc[QA_util_get_last_day(trading_date)]
        target_list = list(set((list(r_tar.index)
                                #pe_list[(pe_list.TARGET.isnull())].reset_index().code.tolist() #+
                                #rrr[(rrr.y_pred==1)&(rrr.TARGET5.isnull())].reset_index().code.tolist() +
                                #find_stock(['880727','880730','880505','880560','880951','880491'])
                                )))
        target_list = [i for i in target_list if i.startswith('688') == False]
        target_pool = prediction_tar.loc[(slice(None),target_list),].loc[QA_util_get_last_day(trading_date)]
    except:
        #r_tar = None
        target_pool = None
    #r_tar = prediction_tar.loc[(slice(None),list(r_tar.index)),].loc[QA_util_get_last_day(trading_date)]
    #per = prediction_tar[(prediction_tar.PASS_MARK.isnull())&(prediction_tar.O_PROB > 0.5)].shape[0]


    #target_pool = pe_list.reset_index().drop_duplicates(subset='NAME',keep='last').sort_values(by=['date','RANK'],ascending=[False,True]).set_index('code')
    per = percent

    #if target_pool[target_pool.y_pred==1].shape[0] > 30:
    #    target_pool = target_pool[target_pool.y_pred==1]



    #pe_list = None
    #if per >= 1:
    #    per = percent
    #else:
    #    per = 0.6

    #if pe_list is None:
    #    #target_pool,prediction,start,end,Model_Date = func(QA_util_get_last_day(trading_date), working_dir, code = list(r_tar.index), type = 'crawl', model_name = 'stock_mars_day')
    #    target_pool = r_tar.sort_values('SKDJ_K_HR')
    #    QA_util_log_info(target_pool)
    #    #target_pool = target_pool.loc[QA_util_get_last_day(trading_date)].reindex(index=r_tar.index).dropna(how='all')
    #else:
    #    #target_pool,prediction,start,end,Model_Date = func(QA_util_get_last_day(trading_date), working_dir, code = list(r_tar.index) + pe_list, type = 'crawl', model_name = 'stock_mars_day')
    #    target_pool = r_tar.append(pe_list).reset_index().drop_duplicates(subset=['code'],keep='first',inplace=False).set_index('code').sort_values('SKDJ_K')
    #    #target_pool = target_pool.loc[QA_util_get_last_day(trading_date)].reindex(index=pe_list.index).dropna(how='all')

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


def trading_new(trading_date, working_dir=working_dir):

    r_tar, prediction_tar2, prediction = load_data(concat_predict, QA_util_get_last_day(trading_date), working_dir, 'stock_xg', 'prediction')
    r_tar, prediction_tar3, prediction = load_data(concat_predict_neut, QA_util_get_last_day(trading_date), working_dir, 'stock_xg_nn', 'prediction_stock_xg_nn')
    r_tar, prediction_tar, prediction = load_data(concat_predict_neut, QA_util_get_last_day(trading_date), working_dir, 'stock_mars_nn', 'prediction_stock_mars_nn')
    r_tar, prediction_tar1, prediction = load_data(concat_predict, QA_util_get_last_day(trading_date), working_dir, 'stock_mars_day', 'prediction_stock_mars_day')

    code_list = list(set(prediction_tar[prediction_tar.RANK <= 20].loc[QA_util_get_last_day(trading_date)].reset_index().code.unique().tolist()
                         + prediction_tar1[prediction_tar1.RANK <= 20].loc[QA_util_get_last_day(trading_date)].reset_index().code.unique().tolist()
                         + prediction_tar2[prediction_tar2.RANK <= 20].loc[QA_util_get_last_day(trading_date)].reset_index().code.unique().tolist()
                         + prediction_tar3[prediction_tar3.RANK <= 20].loc[QA_util_get_last_day(trading_date)].reset_index().code.unique().tolist()
                         ))

    time_list = ['10:00:00', "10:30:00", '11:00:00', "11:30:00", '13:30:00', "14:00:00", '14:30:00', "15:00:00"]

    robot = StrategyRobotBase(code_list, time_list, trading_date)
    robot.set_account(strategy_id)

    strategy = StrategyBase()
    strategy.set_signal_func(signal)
    strategy.set_balance_func(balance)
    strategy.set_percent_func()

    robot.set_strategy(strategy)
    robot.ckeck_market_open()
    robot.get_account()
    robot.run(test=True)

if __name__ == '__main__':
    pass
