
from .setting import working_dir, percent, exceptions
from .concat_predict import concat_predict,concat_predict_real,concat_predict_hedge
from .running import predict_stock_summary
from QUANTTOOLS.Market.MarketTools import trading_base, load_data
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_realtm_ask, QA_fetch_get_stock_indicator_realtime

def trading(trading_date, func = concat_predict, model_name = 'stock_xg', file_name = 'prediction', percent = percent, account= 'name:client-1', working_dir = working_dir, exceptions = exceptions):

    r_tar, prediction_tar, prediction = load_data(func, trading_date, working_dir, model_name, file_name)
    r_tar = prediction_tar[(prediction_tar.RANK <= 20)&(prediction_tar.TARGET5.isnull())].reset_index(level=0, drop=True).drop_duplicates(subset='NAME')
    mark = QA_fetch_get_stock_indicator_realtime(list(r_tar.index), trading_date, trading_date, type = 'hour')
    mark = mark[mark.SKDJ_CROSS2 == 1]
    mark = mark[mark.SKDJ_TR == 1]
    res = trading_base(trading_date, r_tar.loc[mark.index], percent = percent, account= account, title = model_name, exceptions = exceptions)

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

    res = trading_base(trading_date, r_tar, percent = percent, account= account, title = model_name, exceptions = exceptions)

    return(res)

if __name__ == '__main__':
    pass
