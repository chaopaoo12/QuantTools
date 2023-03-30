from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_Client,check_Client,get_Account
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

    def __init__(self, strategy):
        self.strategy = strategy
        self.account = None
        self.exceptions = None
        self.strategy_id = None
        self.percent = None
        self.trader_path = None
        self.client = None
        self.time_list = None
        self.target_list = None
        self.trading_date = None

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

    def set_strategy(self):
        self.time_list = self.strategy.signaltime_list
        self.target_list = self.strategy.target_list
        self.trading_date = self.strategy.trading_date
        QA_util_log_info('##JOB Source Code List ==================== {}'.format(
            self.target_list), ui_log=None)

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

        account_info = get_Account(self.client, self.account)
        # init add data

        # strategy body
        self.strategy = prepare_strategy(self.strategy, {'position': positions,
                                                         'sub_account': sub_accounts,
                                                         })
        self.strategy.set_code_check()
        self.strategy.init_run()

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
            try:
                signal_data = self.strategy.strategy_run(mark_tm)
            except:
                send_actionnotice(self.strategy_id, '程序运行报告:{}'.format(
                    mark_tm), '无法获取信号数据', direction='ERROR', offset='ERROR', volume=None)
                signal_data = {'sell': None, 'buy': None}

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
            if time_check_after("11:30:00") and time_check_before("13:00:00"):
                send_actionnotice(self.strategy_id, '程序运行报告:{}'.format(
                    self.trading_date), '进入午休时段', direction='HOLD', offset='HOLD', volume=None)
            suspend_check(self.trading_date)

            # action
            while check_market_time(test=test) is False:
                if time_check_before('12:56:30'):
                    time.sleep(180)
                elif time_check_before('15:00:00'):
                    time.sleep(3)
                else:
                    break

            if check_market_time() is True:
                QA_util_log_info('##Trading Test Mode {} ==================== {}'.format(test, mark_tm), ui_log=None)
                if signal_data is not None or len(signal_data['sell']) > 0 or len(signal_data['buy']) > 0:
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

                    tm = datetime.datetime.now().strftime("%H:%M:%S")
                    QA_util_log_info(self.time_list[(self.time_list.index(mark_tm)+2) % len(self.time_list)])
                    if tm > self.time_list[(self.time_list.index(mark_tm)+2) % len(self.time_list)]:
                        mark_tm = get_on_time(tm, self.time_list)
                        QA_util_log_info('##已超时 跳入下一交易时段 ==================== {}'.format(mark_tm), ui_log=None)

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