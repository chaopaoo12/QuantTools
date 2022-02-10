from QUANTTOOLS.Market.MarketTools.TimeTools.time_control import time_check_before,time_check_after
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_hour
from QUANTTOOLS.QAStockETL.QAFetch.QAQuantFactor import QA_fetch_get_stock_vwap_min
from QUANTTOOLS.QAStockETL.QAFetch.QATdx import QA_fetch_get_stock_realtime
from QUANTAXIS.QAUtil import QA_util_get_pre_trade_date
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_name,QA_fetch_stock_industryinfo
import time


def signal(buy_list, position, trading_date, mark_tm):

    # 计算信号 提供基础信息 example
    # 输出1 signal 计划持有的code 目前此方案 1:表示持有 0:表示不持有
    # 输出2 signal 进出信号 signal 1:表示进场信号 0:表示无信号 -1:表示卖出信号

    # 提前执行部分
    # 盘前数据准备
    # 午盘数据准备
    if buy_list is None:
        buy_list = []

    if position is not None and position.shape[0] > 0:
        code_list = buy_list + position.code.tolist()
    else:
        code_list = list(set(buy_list))

    # check data time 在某时某刻之后获准获取数据
    while time_check_before(mark_tm):
        time.sleep(60)
        pass

    QA_util_log_info('##JOB Crawl Trading Data ==================== {}'.format(
        mark_tm), ui_log=None)

    # 定时执行部分
    stm = trading_date + ' ' + mark_tm
    source_data = QA_fetch_get_stock_vwap_min(code_list, QA_util_get_pre_trade_date(trading_date,10), trading_date, type='1').sort_index()
    data = source_data.loc[(stm,)]
    price = QA_fetch_get_stock_realtime(code_list)[['涨停价','跌停价','涨跌(%)']].rename({'涨停价':'up_price','跌停价':'down_price','涨跌(%)':'pct_chg'}, axis='columns')
    data = data.join(price)
    QA_util_log_info('##JOB Finished Trading Signal ==================== {}'.format(
        mark_tm), ui_log=None)

    # add information
    # add name industry

    # 方案1
    # hold index&condition
    #下降通道 超降通道 上升通道 超升通道
    #VAMP_C > 15 上升买进 buy_list生效
    #VAMP_C < -15 下降卖出
    #DISTANCE > 0.03 & vamp.abs() < 10 & 未涨停 逃顶
    #DISTANCE < -0.03 & vamp.abs() < 10 & 未跌停 抄底 buy_list生效

    data['signal'] = None
    data['msg'] = None
    data = data.assign(signal = None,
                        msg = None,
                        code = [str(i) for i in data.reset_index().code])


    data.loc[(data.VAMP_JC == 1) & (data.VAMP_C.abs() < 15) & (data.pct_chg < 3) & (data.close < data.up_price), "signal"] = 1
    data.loc[(data.VAMP_JC == 1) & (data.VAMP_C.abs() < 15) & (data.pct_chg < 3) & (data.close < data.up_price), "msg"] = 'VMAP金叉'
    data.loc[(data.VAMP_SC == 1) & (data.VAMP_C.abs() < 15), "signal"] = 0
    data.loc[(data.VAMP_SC == 1) & (data.VAMP_C.abs() < 15), "msg"] = 'VMAP死叉'

    if time_check_after('09:35:00') is True:
        data.loc[(data.VAMP_C >= 15) & (data.close < data.up_price) & (data.DISTANCE < 0.02), "signal"] = 1
        data.loc[(data.VAMP_C >= 15) & (data.close < data.up_price) & (data.DISTANCE < 0.02), "msg"] = '追涨:VMAP上升通道'
        data.loc[data.VAMP_C <= -15, "signal"] = 0
        data.loc[data.VAMP_C <= -15, "msg"] = '止损:VMAP下降通道'

    data.loc[(data.pct_chg < 7) & (data.DISTANCE > 0.03) & (data.VAMP_C.abs() < 10) & (data.close < data.up_price), "signal"] = 0
    data.loc[(data.pct_chg < 7) & (data.DISTANCE > 0.03) & (data.VAMP_C.abs() < 10) & (data.close < data.up_price), "msg"] = 'VMAP超涨'
    data.loc[(data.DISTANCE < -0.03) & (data.VAMP_C > 0) & (data.close > data.down_price), "signal"] = 1
    data.loc[(data.DISTANCE < -0.03) & (data.VAMP_C > 0) & (data.close > data.down_price), "msg"] = 'VMAP超跌'

    data.loc[data.code.isin([i for i in position.code.tolist() if i not in buy_list]) & (data.signal.isin([1])), 'signal'] = None
    #if len([i for i in position.code.tolist() if i not in buy_list]) > 0:
    #    data.loc[[i for i in position.code.tolist() if i not in buy_list]][data.signal == 1, ['signal']] = None
    QA_util_log_info('##Buy DataFrame ====================', ui_log=None)
    QA_util_log_info(data[data.signal == 1], ui_log=None)

    QA_util_log_info('##Sell DataFrame ====================', ui_log=None)
    QA_util_log_info(data[data.signal==0], ui_log=None)

    # 方案2
    #data['signal'] = None
    #data.loc[data.SKDJ_CROSS2_HR == 1, "signal"] = 1
    #data.loc[data.SKDJ_CROSS1_HR == 1, "signal"] = -1
    #data.loc[data.SKDJ_TR_HR == 1, "signal"] = 0

    # msg


    return(data)


def balance(data, position, sub_account, percent):
    # 输入mark(信号标志)&持仓情况&总金额&整体仓位 输出 target_capital&target_position
    # 功能:分配仓位(可以有不同的分仓方案)
    sub_account = 50000

    # 整体仓位可调整percent
    # 细节仓位另算
    if data is not None:
        if position is not None and position.shape[0] > 0:
            data = data.join(position[['code', '市值', '可用余额']].set_index('code'))
            data = data[(data.signal.isin([0, 1])) | (data['可用余额'] > 0)]
            data[['市值','可用余额']] = data[['市值','可用余额']].fillna(0)
        else:
            data = data[(data.signal.isin([0, 1]))]
            data = data.assign(市值=0, 可用余额=0)

        data = data.assign(target_position = data.signal)
                           # 1 / data.signal.sum())
        data = data.assign(target_capital=data.target_position * sub_account * percent)

        # 方案2
        # data = pd.assign(target_position=1 / data.signal.sum(),
        #                 target_capital=data.target_position * sub_account * percent)
        data['industry'] = data['code'].apply(lambda x:QA_fetch_stock_industryinfo(x).SWHY.values[0])
        data['name'] = data['code'].apply(lambda x:QA_fetch_stock_name(x).values[0])
        data['mark'] = None

        data.loc[(data["target_capital"] >= data["市值"]) & (data.signal == 1), "mark"] = "buy"
        data.loc[(data["target_capital"] < data["市值"]) & (data.signal == 0), "mark"] = "sell"
        QA_util_log_info(data, ui_log=None)
        QA_util_log_info('##Buy DataFrame ====================', ui_log=None)
        QA_util_log_info(data[data.mark == 'buy'], ui_log=None)


        QA_util_log_info('##Sell DataFrame ====================', ui_log=None)
        QA_util_log_info(data[data.mark == 'sell'], ui_log=None)

        return(data.reset_index(drop=True).drop_duplicates())
    else:
        return None

def tracking_signal(buy_list, position, trading_date, mark_tm):
    # 计算信号 提供基础信息 example
    # 输出1 signal 计划持有的code 目前此方案 1:表示持有 0:表示不持有
    # 输出2 signal 进出信号 signal 1:表示进场信号 0:表示无信号 -1:表示卖出信号

    # 提前执行部分
    # 盘前数据准备
    # 午盘数据准备
    if buy_list is None:
        buy_list = []

    if position.shape[0] > 0:
        code_list = buy_list + position.code.tolist()
    else:
        code_list = list(set(buy_list))

    # check data time 在某时某刻之后获准获取数据
    while time_check_before(mark_tm):
        time.sleep(60)
        pass

    # 定时执行部分
    stm = trading_date + ' ' + mark_tm
    source_data = QA_fetch_get_stock_vwap_min(QA_util_get_pre_trade_date(trading_date,10), trading_date, code_list, type='1')
    data = source_data.loc[(stm,)]
    # add information
    # add name industry

    # 方案1
    # hold index&condition
    data['signal'] = None
    data.loc[data.SKDJ_CROSS1_HR == 1, "signal"] = -1

    #下降通道 超降通道 上升通道 超升通道


    # 方案2
    #data['signal'] = None
    #data.loc[data.SKDJ_CROSS2_HR == 1, "signal"] = 1
    #data.loc[data.SKDJ_CROSS1_HR == 1, "signal"] = -1
    #data.loc[data.SKDJ_TR_HR == 1, "signal"] = 0

    return(data)

def track_balance(data, buy_list, position, sub_account, percent):
    # 输入mark(信号标志)&持仓情况&总金额&整体仓位 输出 target_capital&target_position
    # 功能:分配仓位(可以有不同的分仓方案)

    # 整体仓位可调整percent
    # 细节仓位另算
    if data is not None:
        if position is not None and position.shape[0] > 0:
            data = data.join(position[['code', '市值', '可用余额']].set_index('code'))
            data = data[(data['signal'] == -1)]
        else:
            return None

        data = data.assign(target_position=0)
        data = data.assign(target_capital=data.target_position * sub_account * percent)

        # 方案2
        # data = pd.assign(target_position=1 / data.signal.sum(),
        #                 target_capital=data.target_position * sub_account * percent)

        data['mark'] = None
        data.loc[data["target_capital"] >= data["市值"], "mark"] = "buy"
        data.loc[data["target_capital"] < data["市值"], "mark"] = "sell"

        return(data.reset_index())
    else:
        return None