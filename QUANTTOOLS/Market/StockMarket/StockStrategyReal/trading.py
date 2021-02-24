
from .setting import working_dir, percent, exceptions
from .concat_predict import concat_predict,concat_predict_real,concat_predict_hedge
from .running import predict_stock_summary
from QUANTTOOLS.Market.MarketTools import trading_base, load_data, trading_base2
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_last_day,QA_util_get_real_date,QA_util_if_trade,QA_util_log_info

def trading(trading_date, func = concat_predict, model_name = 'stock_xg', file_name = 'prediction', percent = percent, account= 'name:client-1', working_dir = working_dir, exceptions = exceptions):

    r_tar, prediction_tar, prediction = load_data(func, QA_util_get_last_day(trading_date), working_dir, model_name, file_name)
    r_tar = prediction_tar[(prediction_tar.RANK <= 20)&(prediction_tar.TARGET5.isnull())].reset_index(level=0, drop=True).drop_duplicates(subset='NAME')
    per = round(r_tar[(r_tar.PASS_MARK.isnull())&(r_tar.O_PROB > 0.5)].shape[0]/20,1)
    QA_util_log_info(r_tar[(r_tar.PASS_MARK.isnull())&(r_tar.O_PROB > 0.5)].shape[0])
    if per < 0.2:
        per = 0.2
    elif per >= 0.6:
        per = percent
    else:
        per = per
    res = trading_base2(trading_date, r_tar, percent = per, account= account, title = model_name, exceptions = exceptions)

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
