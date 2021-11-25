from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_Client,check_Client
from QUANTTOOLS.Trader.account_manage.TradAction.BUY import BUY
from QUANTTOOLS.Trader.account_manage.TradAction.SELL import SELL
import time
import datetime
from QUANTTOOLS.Market.MarketTools.TimeTools.time_control import open_check, close_check, suspend_check, check_market_time, get_on_time


class TradeRobotBase:

    def __init__(self, code_list, time_list, trading_date):
        self.code_list = code_list
        self.time_list = time_list
        self.trading_date = trading_date
        self.strategy_func = None
        self.account = None
        self.exceptions = None
        self.strategy_id = None
        self.percent = None

    def set_strategy_func(self, func):
        self.strategy_func = func

    def set_account(self, strategy_id):
        self.account = strategy_id.account
        self.exceptions = strategy_id.exceptions
        self.strategy_id = strategy_id.strategy_id
        self.percent = strategy_id.percent

    def ckeck_market_open(self):
        open_check(self.trading_date)
        send_actionnotice(self.strategy_id, '交易报告:{}'.format(self.trading_date), '进入交易时段', direction='HOLD', offset='HOLD', volume=None)

    def run(self, test=False):
        # init trading
        QA_util_log_info('##JOB Now Check Timing ==== {}'.format(str(self.trading_date)), ui_log=None)

        # init tm
        tm = datetime.datetime.now().strftime("%H:%M:%S")
        mark_tm = get_on_time(tm, self.time_list)

        # init code
        client = get_Client()
        sub_accounts, frozen, positions, frozen_positions = check_Client(
            client, self.account, self.strategy_id, self.trading_date, exceptions=self.exceptions)
        positions = positions[positions['股票余额'] > 0]

        if self.code_list is None:
            t_list = [] + positions.code.tolist()
        else:
            t_list = list(set(self.code_list + positions.code.tolist()))

        # init add data

        # first time check before 15
        while close_check(self.trading_date):

            client = get_Client()
            sub_accounts, frozen, positions, frozen_positions = check_Client(
                client, self.account, self.strategy_id, self.trading_date, exceptions=self.exceptions)
            positions = positions[positions['股票余额'] > 0]
            account_info = client.get_account(self.account)

            # prepare signal
            StrategyRun = self.strategy_func(t_list, positions, sub_accounts, self.percent, self.trading_date)
            signal_data = self.strategy_func(t_list, positions, sub_accounts, self.percent, self.trading_date, mark_tm)

            # action
            # ckeck whether not market time
            while check_market_time():
                time.sleep(60)
                pass

            for sell_list in signal_data['sell']:
                QA_util_log_info('##JOB Now Start Selling {code} ==== {stm}{msg}'.format(
                    code=sell_list['code'], stm=str(mark_tm), msg=sell_list['msg']), ui_log = None)

                send_actionnotice(self.strategy_id,'{code}{name}:{stm}{msg}'.format(
                    code=sell_list['code'], name=sell_list['name'], stm=mark_tm, msg=sell_list['msg']),
                                  '卖出信号', direction='SELL', offset=mark_tm, volume=None)
                # sell
                SELL(client, self.account, self.strategy_id, account_info, self.trading_date,
                     sell_list['code'], sell_list['name'], sell_list['industry'],
                     target_capital=sell_list['target_capital'], close=0,
                     type='end', test=test)

            for buy_list in signal_data['buy']:
                QA_util_log_info('##JOB Now Start Buying {code} ===== {stm}{msg}'.format(
                    code=buy_list['code'], stm=str(mark_tm), msg=buy_list['msg']), ui_log = None)

                send_actionnotice(self.strategy_id,'{code}{name}:{stm}{msg}'.format(
                    code=buy_list['code'], name=buy_list['name'], stm=mark_tm, msg=buy_list['msg']),
                                  '买入信号', direction='BUY', offset=mark_tm, volume=None)
                # buy
                BUY(client, self.account, self.strategy_id, account_info, self.trading_date,
                    buy_list['code'], buy_list['name'], buy_list['industry'],
                    target_capital=buy_list['target_capital'], close=0,
                    type='end', test=test)
                pass

            # second time check after 9.30
            while open_check(self.trading_date):
                # 盘前停顿
                time.sleep(60)
                pass

            # third time check not suspend
            while suspend_check(self.trading_date):
                # 午盘停顿
                time.sleep(60)
                pass

        QA_util_log_info('交易完成 ==================== {}'.format(
            self.trading_date), ui_log=None)

        send_actionnotice(self.strategy_id, '交易报告:{}'.format(
            self.trading_date), '交易完成', direction='HOLD', offset='HOLD', volume=None)

if __name__ == '__main__':
    pass