from .setting import working_dir, percent, exceptions, strategy_id,trading_setting
from .running import summary_func
from QUANTTOOLS.Market.MarketTools import load_data, StrategyRobotBase, StrategyBase, on_bar
from QUANTAXIS.QAUtil import QA_util_get_last_day,QA_util_get_real_date, QA_util_get_pre_trade_date
from .StrategyOne import signal, balance, tracking_signal, track_balance, code_select

def trading_sim(trading_date, working_dir=working_dir):
    try:
        res,xg,xg_nn,mars_nn,mars_day = summary_func(QA_util_get_pre_trade_date(trading_date,1))

        try:
            res = res.loc[QA_util_get_pre_trade_date(trading_date,1)]
        except:
            res = None

        code_list = list(set(res[(res.y_pred == 1)&(res.RRNG.abs() < 0.1)].reset_index().code.unique().tolist()
                             ))

    except:
        code_list = []

    time_list = on_bar('09:30:00', '15:00:00', 1, [['11:30:00', '13:00:00']])
    time_index = on_bar('09:30:00', '15:00:00', 30, [['11:30:00', '13:00:00']])

    strategy = StrategyBase(target_list=code_list, base_percent=1, trading_date=trading_date)
    strategy.set_codsel_func(code_select, time_index)
    strategy.set_signal_func(signal, time_list)
    strategy.set_balance_func(balance)
    strategy.set_percent_func()

    robot = StrategyRobotBase(strategy)
    robot.set_strategy()
    robot.set_account(strategy_id)
    robot.get_account(type='yun_ease',trader_path=None,host=trading_setting['host'],port=trading_setting['port'],key=trading_setting['key'])

    robot.run(test=False)

def trading_new(trading_date, working_dir=working_dir):
    try:
        res,xg,xg_nn,mars_nn,mars_day = summary_func(QA_util_get_pre_trade_date(trading_date,1))

        try:
            res = res.loc[QA_util_get_pre_trade_date(trading_date,1)]
        except:
            res = None

        code_list = list(set(res[(res.y_pred == 1)&(res.RRNG.abs() < 0.1)].reset_index().code.unique().tolist()
        ))

    except:
        code_list = []

    time_list = on_bar('09:30:00', '15:00:00', 1, [['11:30:00', '13:00:00']])
    time_index = on_bar('09:30:00', '15:00:00', 30, [['11:30:00', '13:00:00']])

    strategy = StrategyBase(target_list=code_list, base_percent=1, trading_date=trading_date)
    strategy.set_codsel_func(code_select, time_index)
    strategy.set_signal_func(signal, time_list)
    strategy.set_balance_func(balance)
    strategy.set_percent_func()

    robot = StrategyRobotBase(strategy)
    robot.set_strategy()
    robot.set_account(strategy_id)
    robot.get_account(trading_setting)

    robot.run(test=False)


def tracking_new(trading_date):

    code_list = None

    time_list = on_bar('09:30:00', '15:00:00', 1, [['11:30:00', '13:00:00']])
    time_index = on_bar('09:30:00', '15:00:00', 30, [['11:30:00', '13:00:00']])

    strategy = StrategyBase(target_list=code_list, base_percent=1, trading_date=trading_date)
    strategy.set_codsel_func(code_select, time_index)
    strategy.set_signal_func(signal, time_list)
    strategy.set_balance_func(balance)
    strategy.set_percent_func()

    robot = StrategyRobotBase(strategy)
    robot.set_strategy()
    robot.set_account(strategy_id)
    robot.get_account(trading_setting)

    robot.run(test=True)


if __name__ == '__main__':
    pass
