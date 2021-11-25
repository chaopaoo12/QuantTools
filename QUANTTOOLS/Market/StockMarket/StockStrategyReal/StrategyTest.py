from QUANTTOOLS.Market.MarketTools.TimeTools.time_control import time_check_before
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_hour
from QUANTAXIS.QAUtil import QA_util_log_info, QA_util_get_pre_trade_date
import time
import pandas as pd


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
    data.loc[data.SKDJ_TR_HR != 1, "signal"] = 0

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

    if position is not None:
        data = data.join(position[['市值', '可用余额']])
        data = data[(data.signal == 1) | (data['可用余额'] > 0)]
    else:
        data = data[(data.signal == 1)]
        data = data.assign(市值=0, 可用余额=0)

    data = data.assign(target_position=1 / data.signal.sum(),
                     target_capital=data.target_position * sub_account * percent)

    # 方案2
    # data = pd.assign(target_position=1 / data.signal.sum(),
    #                 target_capital=data.target_position * sub_account * percent)

    data['mark'] = None
    data.loc[data["target_capital"] >= data["市值"], "mark"] = "buy"
    data.loc[data["target_capital"] < data["市值"], "mark"] = "sell"

    return(data)