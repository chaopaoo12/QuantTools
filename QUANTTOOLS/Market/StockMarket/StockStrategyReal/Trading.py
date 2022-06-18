from .setting import working_dir, percent, exceptions, strategy_id
from .concat_predict import concat_predict, concat_predict_neut
from QUANTTOOLS.Market.MarketTools import load_data, StrategyRobotBase, StrategyBase, on_bar
from QUANTAXIS.QAUtil import QA_util_get_last_day,QA_util_get_real_date, QA_util_get_pre_trade_date
from .StrategyOne import signal, balance, tracking_signal, track_balance, code_select
from QUANTTOOLS.Model.FactorTools.QuantMk import get_index_quant_data,get_quant_data

def trading_new(trading_date, working_dir=working_dir):
    try:
        r_tar, xg, prediction = load_data(concat_predict, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_xg', 'prediction')
        r_tar, xg_nn, prediction = load_data(concat_predict_neut, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_xg_nn', 'prediction_stock_xg_nn')
        r_tar, mars_nn, prediction = load_data(concat_predict_neut, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_mars_nn', 'prediction_stock_mars_nn')
        r_tar, mars_day, prediction = load_data(concat_predict, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_mars_day', 'prediction_stock_mars_day')

        stock_target = get_quant_data(QA_util_get_pre_trade_date(trading_date,1), trading_date, type='model', block=False, sub_block=False,norm_type=None)[['RRNG','RRNG_HR','MA60','MA60_C','MA60_D','RRNG_WK','MA60_C_WK','SHORT10','SHORT20','LONG60','AVG5','MA60_C','SHORT10_WK','SHORT20_WK','LONG60_WK','MA60_C_WK','PASS_MARK','TARGET','TARGET3','TARGET4','TARGET5','TARGET10']]

        xg = xg.join(stock_target[['RRNG','RRNG_HR','MA60','MA60_C','MA60_D','RRNG_WK','MA60_C_WK','SHORT10','SHORT20','LONG60','AVG5','MA60_C','SHORT10_WK','SHORT20_WK','LONG60_WK','MA60_C_WK']])
        xg_nn = xg_nn.join(stock_target[['RRNG','RRNG_HR','MA60','MA60_C','MA60_D','RRNG_WK','MA60_C_WK','SHORT10','SHORT20','LONG60','AVG5','MA60_C','SHORT10_WK','SHORT20_WK','LONG60_WK','MA60_C_WK']])
        mars_nn = mars_nn.join(stock_target[['RRNG','RRNG_HR','MA60','MA60_C','MA60_D','RRNG_WK','MA60_C_WK','SHORT10','SHORT20','LONG60','AVG5','MA60_C','SHORT10_WK','SHORT20_WK','LONG60_WK','MA60_C_WK']])
        mars_day = mars_day.join(stock_target[['RRNG','RRNG_HR','MA60','MA60_C','MA60_D','RRNG_WK','MA60_C_WK','SHORT10','SHORT20','LONG60','AVG5','MA60_C','SHORT10_WK','SHORT20_WK','LONG60_WK','MA60_C_WK']])

        prediction_xg = xg.loc[QA_util_get_pre_trade_date(trading_date,1)]
        prediction_xg_nn = xg_nn.loc[QA_util_get_pre_trade_date(trading_date,1)]
        prediction_mars_nn = mars_nn.loc[QA_util_get_pre_trade_date(trading_date,1)]
        prediction_mars_day = mars_day.loc[QA_util_get_pre_trade_date(trading_date,1)]

        code_list = list(set(prediction_xg[(prediction_xg.y_pred == 1)&(prediction_xg.RRNG.abs() < 0.1)].reset_index().code.unique().tolist()
        + prediction_xg_nn[(prediction_xg_nn.y_pred == 1)&(prediction_xg_nn.RRNG.abs() < 0.1)].reset_index().code.unique().tolist()
        + prediction_mars_nn[(prediction_mars_nn.y_pred == 1)&(prediction_mars_nn.RRNG.abs() < 0.1)].reset_index().code.unique().tolist()
        + prediction_mars_day[(prediction_mars_day.y_pred == 1)&(prediction_mars_day.RRNG.abs() < 0.1)].reset_index().code.unique().tolist()
        ))

    except:
        code_list = None

    time_list = on_bar('09:30:00', '15:00:00', 1, [['11:30:00', '13:00:00']])

    robot = StrategyRobotBase(code_list, time_list, trading_date)
    robot.set_account(strategy_id)

    strategy = StrategyBase(buy_list=code_list, base_percent=1, trading_date=trading_date)
    strategy.set_codsel_func(code_select)
    strategy.set_signal_func(signal)
    strategy.set_balance_func(balance)
    strategy.set_percent_func()

    robot.set_strategy(strategy)
    robot.get_account()
    robot.run(test=False)


def tracking_new(trading_date):

    code_list = None

    #[i.strftime('%H:%M:%S') for i in pd.date_range(start='2019-01-09 09:30:00',end = '2019-01-09 15:00:00',freq='5T')]
    time_list = on_bar('09:30:00', '15:00:00', 1, [['11:30:00', '13:00:00']])

    robot = StrategyRobotBase(code_list, time_list, trading_date)
    robot.set_account(strategy_id)

    strategy = StrategyBase()
    strategy.set_signal_func(tracking_signal)
    strategy.set_balance_func(track_balance)
    strategy.set_percent_func()

    robot.set_strategy(strategy)
    robot.ckeck_market_open()
    robot.get_account()
    robot.run(test=True)


if __name__ == '__main__':
    pass
