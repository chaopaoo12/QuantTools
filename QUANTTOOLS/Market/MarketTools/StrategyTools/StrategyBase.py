from QUANTTOOLS.Market.MarketTools.TimeTools.time_control import time_check_before
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_hour
from QUANTAXIS.QAUtil import QA_util_log_info, QA_util_get_pre_trade_date
import time
import pandas as pd


class StrategyBase:

    def __init__(self, code_list, position, sub_account, base_percent, trading_date):
        self.code_list = code_list
        self.trading_date = trading_date
        self.position = position
        self.sub_account = sub_account
        self.base_percent = base_percent
        self.signal_func = None
        self.balance_func = None
        self.percent_func = None

    def set_signal_func(self, func):
        self.signal_func = func

    def set_balance_func(self, func):
        self.balance_func = func

    def set_percent_func(self, func):
        self.percent_func = func

    def signal_run(self, mark_tm):
        return self.signal_func(self.code_list, self.trading_date, mark_tm)

    def percent_run(self, mark_tm):
        if self.percent_func is not None:
            return self.percent_func(self.code_list, self.trading_date, mark_tm)
        else:
            return self.base_percent

    def balance_run(self, signal_data, percent):
        return self.balance_func(signal_data, self.position, self.sub_account, percent)

    def strategy_run(self, mark_tm):

        data = self.signal_run(mark_tm)

        percent = self.percent_run(mark_tm)

        balance_data = self.balance_run(data, percent)

        signal_data = build_info(balance_data)

        return(signal_data)


def signal(code_list, trading_date, mark_tm):

    # 计算信号 提供基础信息 example
    # 输出1 signal 计划持有的code 目前此方案 1:表示持有 0:表示不持有
    # 输出2 signal 进出信号 signal 1:表示进场信号 0:表示无信号 -1:表示卖出信号

    # 提前执行部分
    # 盘前数据准备
    # 午盘数据准备

    # check data time 在某时某刻之后获准获取数据
    while time_check_before(mark_tm):
        time.sleep(60)
        pass

    # 定时执行部分
    stm = trading_date + ' ' + mark_tm
    source_data = get_quant_data_hour(QA_util_get_pre_trade_date(trading_date,10), trading_date, code_list, type='real').reset_index()
    data = source_data[source_data.datetime == stm].set_index('code')

    # add information
    # add name industry

    # 方案1
    # hold index&condition
    data['signal'] = None
    data.loc[data.SKDJ_TR_HR == 1, "signal"] = 1
    data.loc[~data.SKDJ_TR_HR == 1, "signal"] = 0

    # 方案2
    #data['signal'] = None
    #data.loc[data.SKDJ_CROSS2_HR == 1, "signal"] = 1
    #data.loc[data.SKDJ_CROSS1_HR == 1, "signal"] = -1
    #data.loc[data.SKDJ_TR_HR == 1, "signal"] = 0

    return(data)


def balance(data, position, sub_account, percent):
    # 输入mark(信号标志)&持仓情况&总金额&整体仓位 输出 target_capital&target_position
    # 功能:分配仓位(可以有不同的分仓方案)

    # 整体仓位可调整percent
    # 细节仓位另算

    data = data.join(position[['市值', '可用余额']])
    data = data[(data.signal == 1) | (data['可用余额'] > 0)]

    data = pd.assign(target_position=1 / data.signal.sum(),
                     target_capital=data.target_position * sub_account * percent)

    # 方案2
    # data = pd.assign(target_position=1 / data.signal.sum(),
    #                 target_capital=data.target_position * sub_account * percent)

    data['mark'] = None
    data.loc[data["target_capital"] >= data["市值"], "mark"] = "buy"
    data.loc[data["target_capital"] < data["市值"], "mark"] = "sell"

    return(data)


def build_info(data):
    # 数据规整化 固定的工具函数
    # 标准数据格式
    # signal = {'sell':[{'code':'000001', 代码
    #                   'hold':0, 持仓量
    #                   'name':'xx', 名称
    #                   'industry':'xxxx', 行业
    #                   'msg':'xxx', 消息
    #                   'close':0, 昨日收盘价 默认0 抢板可使用
    #                   'target_position':0, 分配比例
    #                   'target_capital':0,目标持仓金额 balance结果
    #                   'data':'xxxx'}, 原始数据
    #                  {...}],
    #          'buy':[{'code':'000004',
    #                  'hold':0,
    #                  'name':'xx',
    #                  'industry':'xxxx',
    #                  'msg':'xxx',
    #                  'close':0,
    #                  'target_position':0,
    #                  'target_capital':0,目标持仓金额
    #                  'data':'xxxx'},
    #                 {...}]}

    QA_util_log_info('##CHECK columns ', ui_log = None)
    need_columns = ['code', 'name', 'industry', 'msg', 'close', 'target_position', 'target_capital', 'mark']
    for inset_column in [i for i in need_columns if i not in data.columns]:
        QA_util_log_info('##CHECK short of columns {}'.format(inset_column), ui_log = None)
        data = pd.assign(inset_column=0)

    sell_list = data[data.mark == 'sell'].code.tolist()
    sell_dict = data[data.code.isin(sell_list)][need_columns].to_dict(orient='records')
    buy_list = data[data.mark == 'buy'].code.tolist()
    buy_dict = data[data.code.isin(buy_list)][need_columns].to_dict(orient='records')

    signal_data = {'sell': sell_dict, 'buy': buy_dict}
    return(signal_data)