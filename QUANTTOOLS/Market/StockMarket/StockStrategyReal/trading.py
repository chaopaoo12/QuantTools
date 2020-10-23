
from .setting import working_dir, percent, exceptions
from .concat_predict import concat_predict,concat_predict_real,concat_predict_hedge
from QUANTTOOLS.Market.MarketTools import trading_base, load_data
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_realtm_ask

def trading(trading_date, func = concat_predict, model_name = 'stock_xg', file_name = 'prediction', percent = percent, account= 'name:client-1', working_dir = working_dir, exceptions = exceptions):

    r_tar, prediction_tar = load_data(func, trading_date, working_dir, model_name, file_name)

    res = trading_base(trading_date, r_tar, prediction_tar, percent = percent, account= account, title = model_name, exceptions = exceptions)

    return(res)

def trading_real(trading_date, func = concat_predict, model_name = 'stock_xg_real', file_name = 'prediction_real', percent = percent, account= 'name:client-1', working_dir = working_dir, exceptions = exceptions):

    r_tar, prediction_tar = load_data(func, trading_date, working_dir, model_name, file_name)

    res = trading_base(trading_date, r_tar, prediction_tar, percent = percent, account= account, title = model_name, exceptions = exceptions)

    return(res)

def trading_hedge(trading_date, func = concat_predict, model_name = 'hedge_xg', file_name = 'prediction_hedge', percent = percent, account= 'name:client-1', working_dir = working_dir, exceptions = exceptions):

    r_tar, prediction_tar = load_data(func, trading_date, working_dir, model_name, file_name)

    res = trading_base(trading_date, r_tar, prediction_tar, percent = percent, account= account, title = model_name, exceptions = exceptions)

    return(res)


def trading_summary(trading_date,percent = percent, account= 'name:client-1',exceptions = exceptions):

    r_tar, prediction_tar = load_data(concat_predict, trading_date, working_dir, 'stock_xg_real', 'prediction_real')
    model_name = 'stock_xg_real'

    buy_code = []
    for i in list(r_tar.index):
        if QA_fetch_get_stock_realtm_ask(i) > 0:
            buy_code = buy_code.append(i)
    print(buy_code)

    if buy_code is None:
        r_tar, prediction_tar = load_data(concat_predict, trading_date, working_dir, 'stock_xg', 'prediction')
        model_name = 'stock_xg'


    res = trading_base(trading_date, r_tar, prediction_tar, percent = percent, account= account, title = model_name, exceptions = exceptions)

    return(res)

if __name__ == '__main__':
    pass
