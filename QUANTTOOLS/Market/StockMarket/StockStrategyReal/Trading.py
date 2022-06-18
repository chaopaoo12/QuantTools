from .setting import working_dir, percent, exceptions, strategy_id
from .running import summary_func
from QUANTTOOLS.Market.MarketTools import load_data, StrategyRobotBase, StrategyBase, on_bar
from QUANTAXIS.QAUtil import QA_util_get_last_day,QA_util_get_real_date, QA_util_get_pre_trade_date
from .StrategyOne import signal, balance, tracking_signal, track_balance, code_select

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
