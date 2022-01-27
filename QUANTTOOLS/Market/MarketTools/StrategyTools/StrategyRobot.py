from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_Client,check_Client
import time
import datetime
from QUANTTOOLS.Market.MarketTools.TimeTools.time_control import open_check, close_check, suspend_check, get_on_time,time_check_before, check_market_time,time_check_after
from QUANTTOOLS.Market.MarketTools.TradingTools.trading_robot import trading_robot


def prepare_strategy(strategy, args_dict: dict):
    for k, v in args_dict.items():
        QA_util_log_info('{} : {}'.format(k, v))
        setattr(strategy, k, v)
    return(strategy)


class StrategyRobotBase:
    # 整合

    def __init__(self, code_list, time_list, trading_date):
        self.code_list = code_list
        self.time_list = time_list
        self.trading_date = trading_date
        self.strategy = None
        self.account = None
        self.exceptions = None
        self.strategy_id = None
        self.percent = None
        self.trader_path = None
        self.client = None

    def set_account(self, strategy_id):
        self.account = strategy_id['account']
        self.exceptions = strategy_id['exceptions']
        self.strategy_id = strategy_id['strategy_id']
        self.percent = strategy_id['percent']
        self.trader_path = strategy_id['trader_path']

    def get_account(self, trader_path=None):
        if self.trader_path is not None:
            pass
        else:
            self.trader_path = trader_path
        self.client = get_Client(trader_path=self.trader_path)


    def set_strategy(self, strategy):
        self.strategy = strategy

    def ckeck_market_open(self):
        open_check(self.trading_date)
        send_actionnotice(self.strategy_id, '交易报告:{}'.format(
            self.trading_date), '进入交易时段', direction='HOLD', offset='HOLD', volume=None)

    def run(self, test=False):
        # init trading
        QA_util_log_info('##JOB Now Check Timing ==== {}'.format(str(self.trading_date)), ui_log=None)

        # init tm
        tm = datetime.datetime.now().strftime("%H:%M:%S")
        mark_tm = get_on_time(tm, self.time_list)

        # init code
        QA_util_log_info('##JOB Now Init Code List ==== {}'.format(str(self.trading_date)), ui_log=None)
        sub_accounts, frozen, positions, frozen_positions = check_Client(
            self.client, self.account, self.strategy_id, self.trading_date, exceptions=self.exceptions)

        if positions.shape[0] > 0:
            positions = positions[positions['股票余额'] > 0]
        else:
            pass

        if self.code_list is None:
            self.code_list = []

        QA_util_log_info('##Code List ==== {}'.format(str(self.trading_date)), ui_log=None)
        QA_util_log_info(self.code_list, ui_log=None)

        account_info = self.client.get_account(self.account)
        # init add data

        # first time check before 15
        while time_check_before('15:00:00', test=test):

            sub_accounts, frozen, positions, frozen_positions = check_Client(
                self.client, self.account, self.strategy_id, self.trading_date, exceptions=self.exceptions)

            if positions.shape[0] > 0:
                positions = positions[positions['股票余额'] > 0]
            else:
                pass

            # strategy body
            self.strategy = prepare_strategy(self.strategy, {'buy_list': self.code_list,
                                                             'trading_date': self.trading_date,
                                                             'position': positions,
                                                             'sub_account': sub_accounts,
                                                             'base_percent': self.percent,
                                                             })

            # prepare signal
            signal_data = self.strategy.strategy_run(mark_tm)
            QA_util_log_info('##Sell List ==== {}'.format(str(self.trading_date)), ui_log=None)
            QA_util_log_info(signal_data['sell'], ui_log=None)

            QA_util_log_info('##Buy List ==== {}'.format(str(self.trading_date)), ui_log=None)
            QA_util_log_info(signal_data['buy'], ui_log=None)

            # second time check after 9.30
            # 盘前停顿
            open_check(self.trading_date)

            # third time check not suspend
            # 午盘停顿
            suspend_check(self.trading_date)

            # action
            while check_market_time(test=test) is False:
                if time_check_before('15:00:00'):
                    time.sleep(60)
                else:
                    break

            trading_robot(self.client, self.account, account_info, signal_data,
                          self.trading_date, mark_tm, self.strategy_id, test=test)

            if time_check_after('15:00:00') and test is True:
                break
            else:
                # get next mark_tm
                mark_tm = self.time_list[(self.time_list.index(mark_tm)+1) % len(self.time_list)]
                pass

        QA_util_log_info('当日交易完成 ==================== {}'.format(
            self.trading_date), ui_log=None)

        send_actionnotice(self.strategy_id, '交易报告:{}'.format(
            self.trading_date), '当日交易完成', direction='HOLD', offset='HOLD', volume=None)


if __name__ == '__main__':
    pass