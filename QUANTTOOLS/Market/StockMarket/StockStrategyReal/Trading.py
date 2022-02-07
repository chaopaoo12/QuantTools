from .setting import working_dir, percent, exceptions, strategy_id
from .concat_predict import concat_predict, concat_predict_neut
from QUANTTOOLS.Market.MarketTools import load_data, StrategyRobotBase, StrategyBase
from QUANTAXIS.QAUtil import QA_util_get_last_day,QA_util_get_real_date, QA_util_get_pre_trade_date
from .StrategyOne import signal, balance, tracking_signal, track_balance


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

    time_list = ['09:30:00',
                 '09:35:00',
                 '09:40:00',
                 '09:45:00',
                 '09:50:00',
                 '09:55:00',
                 '10:00:00',
                 '10:05:00',
                 '10:10:00',
                 '10:15:00',
                 '10:20:00',
                 '10:25:00',
                 '10:30:00',
                 '10:35:00',
                 '10:40:00',
                 '10:45:00',
                 '10:50:00',
                 '10:55:00',
                 '11:00:00',
                 '11:05:00',
                 '11:10:00',
                 '11:15:00',
                 '11:20:00',
                 '11:25:00',
                 '11:30:00',
                 '13:00:00',
                 '13:05:00',
                 '13:10:00',
                 '13:15:00',
                 '13:20:00',
                 '13:25:00',
                 '13:30:00',
                 '13:35:00',
                 '13:40:00',
                 '13:45:00',
                 '13:50:00',
                 '13:55:00',
                 '14:00:00',
                 '14:05:00',
                 '14:10:00',
                 '14:15:00',
                 '14:20:00',
                 '14:25:00',
                 '14:30:00',
                 '14:35:00',
                 '14:40:00',
                 '14:45:00',
                 '14:50:00',
                 '14:55:00',
                 '15:00:00']

    robot = StrategyRobotBase(code_list, time_list, trading_date)
    robot.set_account(strategy_id)

    strategy = StrategyBase()
    strategy.set_signal_func(signal)
    strategy.set_balance_func(balance)
    strategy.set_percent_func()

    robot.set_strategy(strategy)
    robot.ckeck_market_open()
    robot.get_account()
    robot.run(test=False)


def tracking_new(trading_date):

    code_list = None

    #[i.strftime('%H:%M:%S') for i in pd.date_range(start='2019-01-09 09:30:00',end = '2019-01-09 15:00:00',freq='5T')]
    time_list = ['09:30:00',
                 '09:35:00',
                 '09:40:00',
                 '09:45:00',
                 '09:50:00',
                 '09:55:00',
                 '10:00:00',
                 '10:05:00',
                 '10:10:00',
                 '10:15:00',
                 '10:20:00',
                 '10:25:00',
                 '10:30:00',
                 '10:35:00',
                 '10:40:00',
                 '10:45:00',
                 '10:50:00',
                 '10:55:00',
                 '11:00:00',
                 '11:05:00',
                 '11:10:00',
                 '11:15:00',
                 '11:20:00',
                 '11:25:00',
                 '11:30:00',
                 '13:00:00',
                 '13:05:00',
                 '13:10:00',
                 '13:15:00',
                 '13:20:00',
                 '13:25:00',
                 '13:30:00',
                 '13:35:00',
                 '13:40:00',
                 '13:45:00',
                 '13:50:00',
                 '13:55:00',
                 '14:00:00',
                 '14:05:00',
                 '14:10:00',
                 '14:15:00',
                 '14:20:00',
                 '14:25:00',
                 '14:30:00',
                 '14:35:00',
                 '14:40:00',
                 '14:45:00',
                 '14:50:00',
                 '14:55:00',
                 '15:00:00']

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
