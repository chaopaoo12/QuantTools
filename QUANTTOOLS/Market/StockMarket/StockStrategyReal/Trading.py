from .setting import working_dir, percent, exceptions, strategy_id
from .concat_predict import concat_predict, concat_predict_neut
from QUANTTOOLS.Market.MarketTools import load_data, StrategyRobotBase, StrategyBase, on_bar
from QUANTAXIS.QAUtil import QA_util_get_last_day,QA_util_get_real_date, QA_util_get_pre_trade_date
from .StrategyOne import signal, balance, tracking_signal, track_balance


def trading_new(trading_date, working_dir=working_dir):
    try:
        #r_tar, prediction_tar2, prediction = load_data(concat_predict, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_xg', 'prediction')
        r_tar, prediction_tar3, prediction = load_data(concat_predict_neut, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_xg_nn', 'prediction_stock_xg_nn')
        r_tar, prediction_tar, prediction = load_data(concat_predict_neut, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_mars_nn', 'prediction_stock_mars_nn')
        #r_tar, prediction_tar1, prediction = load_data(concat_predict, QA_util_get_pre_trade_date(trading_date,1), working_dir, 'stock_mars_day', 'prediction_stock_mars_day')

        code_list = list(set(prediction_tar[prediction_tar.y_pred == 1].loc[QA_util_get_pre_trade_date(trading_date,1)].reset_index().code.unique().tolist()
        #+ prediction_tar1[prediction_tar1.RANK <= 20].loc[QA_util_get_pre_trade_date(trading_date,1)].reset_index().code.unique().tolist()
        #+ prediction_tar2[prediction_tar2.RANK <= 20].loc[QA_util_get_pre_trade_date(trading_date,1)].reset_index().code.unique().tolist()
        #+ prediction_tar3[(prediction_tar3.y_pred == 1) & prediction_tar3.TARGET3.isnull()].reset_index().code.unique().tolist()
        ))

        if len(code_list) == 0:
            pass
        else:
            code_list = list(set(prediction_tar[prediction_tar.y_pred == 1].loc[QA_util_get_pre_trade_date(trading_date,1)].reset_index().code.unique().tolist()
                             #+ prediction_tar1[prediction_tar1.RANK <= 20].loc[QA_util_get_pre_trade_date(trading_date,1)].reset_index().code.unique().tolist()
                             #+ prediction_tar2[prediction_tar2.RANK <= 20].loc[QA_util_get_pre_trade_date(trading_date,1)].reset_index().code.unique().tolist()
                             + prediction_tar3[(prediction_tar3.y_pred == 1) & prediction_tar3.TARGET3.isnull()].reset_index().code.unique().tolist()
                             ))
    except:
        code_list = None

    time_list = on_bar('09:30:00', '15:00:00', 1, [['11:30:00', '13:00:00']])

    robot = StrategyRobotBase(code_list, time_list, trading_date)
    robot.set_account(strategy_id)

    strategy = StrategyBase(buy_list=code_list, base_percent=1, trading_date=trading_date)
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
