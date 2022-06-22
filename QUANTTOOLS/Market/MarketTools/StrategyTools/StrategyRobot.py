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

    def get_account(self,type='yun_ease',**kwargs):
        try:
            trader_path=kwargs['path']
        except:
            trader_path=None

        try:
            host=kwargs['host']
            port=kwargs['port']
            key=kwargs['key']
        except:
            host=None
            port=None
            key=None
        try:
            token=kwargs['token']
            server=kwargs['server']
            account=kwargs['account']
            name=kwargs['name']
        except:
            token=None
            server=None
            account=None
            name=None

        self.client = get_Client(type,
                                 trader_path=trader_path,host=host,port=port,key=key,
                                 token=token,server=server,account=account,name=name)


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
        if mark_tm == '15:00:00':
            mark_tm = self.time_list[0]
        QA_util_log_info('##JOB Now Init Mark Time ==== {}'.format(str(mark_tm)), ui_log=None)

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

        self.tmp_list = None

        QA_util_log_info('##Code List ==== {}'.format(str(self.trading_date)), ui_log=None)
        QA_util_log_info(self.code_list, ui_log=None)

        account_info = self.client.get_account(self.account)
        # init add data

        # strategy body
        self.strategy = prepare_strategy(self.strategy, {'buy_list': self.code_list,
                                                         'trading_date': self.trading_date,
                                                         'position': positions,
                                                         'sub_account': sub_accounts,
                                                         'base_percent': self.percent,
                                                         'tmp_list': self.tmp_list
                                                         })

        # first time check before 15
        while time_check_before('15:00:00', test=test):

            while time_check_before(mark_tm):
                time.sleep(3)
            QA_util_log_info('##JOB On Time ==== {}'.format(mark_tm), ui_log=None)

            sub_accounts, frozen, positions, frozen_positions = check_Client(
                self.client, self.account, self.strategy_id, self.trading_date, exceptions=self.exceptions)

            if positions.shape[0] > 0:
                positions = positions[positions['股票余额'] > 0]
            else:
                pass

            #refresh strategy body
            self.strategy = prepare_strategy(self.strategy, {'position': positions,
                                                             'sub_account': sub_accounts
                                                             })

            # prepare signal.

            signal_data = self.strategy.strategy_run(mark_tm)
            QA_util_log_info('##Sell List ==== {}'.format(str(self.trading_date)), ui_log=None)
            QA_util_log_info(signal_data['sell'], ui_log=None)

            QA_util_log_info('##Buy List ==== {}'.format(str(self.trading_date)), ui_log=None)
            QA_util_log_info(signal_data['buy'], ui_log=None)

            # second time check after 9.30
            # 盘前停顿
            if time_check_before('09:26:30'):
                open_check(self.trading_date, 180)
            else:
                open_check(self.trading_date, 15)

            # third time check not suspend
            # 午盘停顿
            suspend_check(self.trading_date)

            # action
            while check_market_time(test=test) is False:
                if time_check_before('12:56:30'):
                    time.sleep(180)
                elif time_check_before('15:00:00'):
                    time.sleep(3)
                else:
                    break

            if check_market_time(test=test) is True:
                QA_util_log_info('##Trading Test Mode {} ==================== {}'.format(test, mark_tm), ui_log=None)
                if signal_data['sell'] is not None or signal_data['buy'] is not None:
                    trading_robot(self.client, self.account, account_info, signal_data,
                                  self.trading_date, mark_tm, self.strategy_id, test=test)
                else:
                    QA_util_log_info('##JOB 无交易信号 ==================== {}'.format(mark_tm), ui_log=None)
            else:
                QA_util_log_info('##JOB 已过交易时间 ==================== {}'.format(mark_tm), ui_log=None)

            if time_check_before('15:00:00'):
                # get next mark_tm
                QA_util_log_info('##本交易时段 ==================== {}'.format(mark_tm), ui_log=None)
                if mark_tm <= self.time_list[(self.time_list.index(mark_tm)+1) % len(self.time_list)]:
                    mark_tm = self.time_list[(self.time_list.index(mark_tm)+1) % len(self.time_list)]
                    QA_util_log_info('##下一交易时段 ==================== {}'.format(mark_tm), ui_log=None)
                else:
                    QA_util_log_info('##JOB 已过交易时间 ==================== {}'.format(mark_tm), ui_log=None)
                    break

        if time_check_after('15:00:00'):
            QA_util_log_info('当日交易完成 ==================== {}'.format(
                self.trading_date), ui_log=None)
            send_actionnotice(self.strategy_id, '交易报告:{}'.format(
                self.trading_date), '当日交易完成', direction='HOLD', offset='HOLD', volume=None)





if __name__ == '__main__':
    pass