from .setting import working_dir, percent, exceptions, strategy_id,trading_setting
from .concat_predict import (concat_predict,concat_predict_neut)
from .running import summary_func,watch_func
from QUANTTOOLS.Model.FactorTools.QuantMk import get_index_quant_data,get_quant_data,get_quant_data_15min
from QUANTTOOLS.Market.MarketTools import load_data, StrategyRobotBase, StrategyBase, on_bar
from QUANTAXIS.QAUtil import QA_util_get_last_day,QA_util_get_real_date, QA_util_get_pre_trade_date
#from .StrategyOne import signal, balance, tracking_signal, track_balance, code_select
from .StrategySec import signal, balance, tracking_signal, track_balance, code_select, day_init


def trading_sim(trading_date, working_dir=working_dir):
    r_tar, xg, prediction = load_data(concat_predict, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_xg', 'prediction')
    r_tar, xg_nn, prediction = load_data(concat_predict_neut, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_xg_nn', 'prediction_stock_xg_nn')
    r_tar, mars_nn, prediction = load_data(concat_predict_neut, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_mars_nn', 'prediction_stock_mars_nn')
    r_tar, mars_day, prediction = load_data(concat_predict, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_mars_day', 'prediction_stock_mars_day')

    xg=xg.loc[QA_util_get_pre_trade_date(trading_date,1)]
    xg_nn=xg_nn.loc[QA_util_get_pre_trade_date(trading_date,1)]
    mars_nn=mars_nn.loc[QA_util_get_pre_trade_date(trading_date,1)]
    mars_day=mars_day.loc[QA_util_get_pre_trade_date(trading_date,1)]

    xg=xg[xg.y_pred==1]
    xg_nn=xg_nn[xg_nn.y_pred==1]
    mars_nn=mars_nn[mars_nn.y_pred==1]
    mars_day=mars_day[mars_day.y_pred==1]

    res_a, res_b, res_c, res_d = watch_func(QA_util_get_pre_trade_date(trading_date,1), QA_util_get_pre_trade_date(trading_date,1))

    stock_list = list(set(xg.reset_index().code.tolist() + xg_nn.reset_index().code.tolist() +
                          mars_nn.reset_index().code.tolist() + mars_day.reset_index().code.tolist()))

    stock_target = get_quant_data(QA_util_get_pre_trade_date(trading_date,1), QA_util_get_pre_trade_date(trading_date,1),
                                  list(set(xg.reset_index().code.tolist() + xg_nn.reset_index().code.tolist() +
                                           mars_nn.reset_index().code.tolist() + mars_day.reset_index().code.tolist()
                                           + res_b.reset_index().code.tolist())),
                                  type='crawl', block=False, sub_block=False,norm_type=None)

    res_b = res_b.join(stock_target[['RRNG','RRNG_WK']])

    code_list = list(set(stock_target[stock_target['RRNG_WK'].abs() < 0.1].loc[(QA_util_get_pre_trade_date(trading_date,1),stock_list),].reset_index().code.tolist()
                         + res_b[(res_b.RRNG_WK.abs() < 0.1)&(res_b.PB <= res_b.I_PB * 0.8)&(res_b.PE_TTM <= res_b.I_PE * 0.8)&(res_b.PE_TTM > 0)&(res_b.TM_RATE < -0.5)].reset_index().code.tolist()))

    time_list = on_bar('09:30:00', '15:00:00', 1, [['11:30:00', '13:00:00']])
    time_index = on_bar('09:30:00', '15:00:00', 15, [['11:30:00', '13:00:00']])

    strategy = StrategyBase(target_list=code_list, base_percent=1, trading_date=trading_date)
    strategy.set_init_func(day_init)
    strategy.set_codsel_func(code_select, time_index)
    strategy.set_signal_func(signal, time_list)
    strategy.set_balance_func(balance)
    strategy.set_percent_func()

    robot = StrategyRobotBase(strategy)
    robot.set_strategy()
    robot.set_account(strategy_id)
    robot.get_account(type='sim_myquant',trader_path=None,
                      token=trading_setting['token'],server=trading_setting['server'],account=trading_setting['account'],name=trading_setting['name'])
    robot.run(test=False)

def trading_new(trading_date, working_dir=working_dir):
    r_tar, xg_sh, prediction = load_data(concat_predict, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_sh', 'prediction_sh')
    r_tar, xg, prediction = load_data(concat_predict, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_xg', 'prediction')
    #r_tar, xg_nn, prediction = load_data(concat_predict_neut, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_xg_nn', 'prediction_stock_xg_nn')
    #r_tar, mars_nn, prediction = load_data(concat_predict_neut, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_mars_nn', 'prediction_stock_mars_nn')
    r_tar, mars_day, prediction = load_data(concat_predict, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_mars_day', 'prediction_stock_mars_day')

    #xg_sh=xg_sh[(xg_sh.RANK<=20)&(xg_sh.O_PROB>=0.35)]

    code_list = list(set(xg_sh[(xg_sh.RANK <= 20)&(xg_sh.TARGET5.isnull())&(xg_sh.SKDJ_K<50)].reset_index().code.tolist()
                         + xg[(xg.RANK <= 20)&(~xg.INDUSTRY.isin(['银行']))&(xg.TARGET5.isnull())&(xg.SKDJ_K<50)].reset_index().code.tolist()
                         #+ xg_nn[(xg_nn.RANK <= 5)&(~xg_nn.INDUSTRY.isin(['银行']))&(xg_nn.y_pred==1)&(xg_nn.TARGET5.isnull())].reset_index().code.tolist()
                         #+ mars_nn[(mars_nn.RANK <= 5)&(~mars_nn.INDUSTRY.isin(['银行']))&(mars_nn.y_pred==1)&(mars_nn.TARGET5.isnull())].reset_index().code.tolist()
                         + mars_day[(mars_day.RANK <= 20)&(~mars_day.INDUSTRY.isin(['银行']))&(mars_day.TARGET5.isnull())&(mars_day.SKDJ_K<50)].reset_index().code.tolist()))
    time_list = on_bar('09:30:00', '15:00:00', 1, [['11:30:00', '13:00:00']])
    time_index = on_bar('09:30:00', '15:00:00', 15, [['11:30:00', '13:00:00']])

    strategy = StrategyBase(target_list=code_list, base_percent=1, trading_date=trading_date)
    strategy.set_init_func(day_init)
    strategy.set_codsel_func(code_select, time_index)
    strategy.set_signal_func(signal, time_list)
    strategy.set_balance_func(balance)
    strategy.set_percent_func()

    robot = StrategyRobotBase(strategy)
    robot.set_strategy()
    robot.set_account(strategy_id)
    robot.get_account(type='yun_ease',trader_path=None,host=trading_setting['host'],port=trading_setting['port'],key=trading_setting['key'])

    robot.run(test=False)


def tracking_new(trading_date):

    code_list = None

    time_list = on_bar('09:30:00', '15:00:00', 1, [['11:30:00', '13:00:00']])
    time_index = on_bar('09:30:00', '15:00:00', 15, [['11:30:00', '13:00:00']])

    strategy = StrategyBase(target_list=code_list, base_percent=1, trading_date=trading_date)
    strategy.set_codsel_func(code_select, time_index)
    strategy.set_signal_func(signal, time_list)
    strategy.set_balance_func(balance)
    strategy.set_percent_func()

    robot = StrategyRobotBase(strategy)
    robot.set_strategy()
    robot.set_account(strategy_id)
    robot.get_account(type='yun_ease',trader_path=None,host=trading_setting['host'],port=trading_setting['port'],key=trading_setting['key'])

    robot.run(test=True)


if __name__ == '__main__':
    pass
