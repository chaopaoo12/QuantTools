from QUANTTOOLS.Market.MarketTools.TimeTools.time_control import time_check_before,time_check_after
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_vwap
from QUANTTOOLS.Model.FactorTools.QuantMk import get_quant_data_30min,get_quant_data_15min
from QUANTAXIS.QAUtil import QA_util_get_pre_trade_date
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_name,QA_fetch_stock_industryinfo
import time
import numpy as np
from QUANTTOOLS.Model.StockModel.StrategyXgboostMin import QAStockXGBoostMin
from QUANTTOOLS.QAStockETL.QAData import QA_DataStruct_Stock_min
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_min_adv
from QUANTAXIS.QAUtil import QA_util_get_pre_trade_date
from QUANTTOOLS.QAStockETL.QAFetch.QAIndicator import get_indicator
import pandas as pd

def data_base(code_list,trading_date):
    source_data = QA_fetch_get_stock_vwap(code_list, QA_util_get_pre_trade_date(trading_date,10), trading_date, type='real')
    last = source_data.groupby(['date','code'])['close'].agg([('last1','last')])
    last = last.assign(yes_close=last.groupby('code').shift())
    data = source_data.reset_index().set_index(['date','code']).join(last).reset_index().set_index(['datetime','code'])
    data = data.assign(TARGET = data.day_close/data.last1-1,
                       pct= data.day_close/data.yes_close-1)
    return(data)

def data_collect(code_list, trading_date, day_temp_data, sec_temp_data, source_data):
    #try:
    if source_data is None:
        source_data = data_base(code_list, trading_date)
    print('source_data')
    print(source_data)

    print('sec_temp_data')
    print(sec_temp_data)

    data = source_data.join(sec_temp_data)
    data = data.groupby('code').fillna(method='ffill')

    stock_model = QAStockXGBoostMin()
    stock_model = stock_model.load_model('stock_in')
    stock_model.set_data(data)
    stock_model.base_predict()
    data[['IN_SIG','IN_PROB']] = stock_model.data[['y_pred','O_PROB']]

    stock_model = stock_model.load_model('stock_out')
    stock_model.set_data(data)
    stock_model.base_predict()
    data[['OUT_SIG','OUT_PROB']] = stock_model.data[['y_pred','O_PROB']]
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
    QA_util_log_info('##JOB Out Signal Decide ====================', ui_log=None)
    #顶部死叉
    data.loc[(data.OUT_SIG == 1) & (data.IN_SIG == 0),"signal"] = 0
    data.loc[(data.OUT_SIG == 1) & (data.IN_SIG == 0),"msg"] = 'model出场信号'

    # 强制止损
    data.loc[(data.pct_chg <= -5) & (data.IN_SIG == 0), "signal"] = 0
    data.loc[(data.pct_chg <= -5) & (data.IN_SIG == 0), "msg"] = '强制止损'
    QA_util_log_info('##JOB Int Signal Decide ====================', ui_log=None)
    #放量金叉
    data.loc[(data.IN_SIG == 1) & (data.OUT_SIG == 0), "signal"] = 1
    data.loc[(data.IN_SIG == 1) & (data.OUT_SIG == 0), "msg"] = 'model进场信号'

    return(data, [sec_temp_data])
    #except:
    #    return(None, [sec_temp_data])

def day_init(target_list, trading_date):

    QA_util_log_info('##JOB Init Day Data ==================== {}'.format(
        trading_date), ui_log=None)

    his15_data= QA_fetch_stock_min_adv(target_list,QA_util_get_pre_trade_date(trading_date,10),
                                       QA_util_get_pre_trade_date(trading_date,5),'15min')
    his30_data= QA_fetch_stock_min_adv(target_list,QA_util_get_pre_trade_date(trading_date,10),
                                       QA_util_get_pre_trade_date(trading_date,5),'30min')

    return([his15_data, his30_data])

def code_select(target_list, position, day_temp_data, sec_temp_data, trading_date, mark_tm):

    if target_list is None:
        target_list = []

    if position is not None and position.shape[0] > 0:
        code_list = target_list + position.code.tolist()
    else:
        code_list = target_list

    QA_util_log_info('##JOB Refresh Code List ==================== {}'.format(
        mark_tm), ui_log=None)

    source_data = data_base(code_list, trading_date)

    if sec_temp_data is None:
        temp = source_data.assign(type='1min')
        temp = QA_DataStruct_Stock_min(temp)
        res15=pd.concat([temp.min15, day_temp_data[0].data])
        res30=pd.concat([temp.min30, day_temp_data[1].data])
        res15 = res15[~res15.index.duplicated(keep='first')]
        res30 = res30[~res30.index.duplicated(keep='first')]
        res15=get_indicator(QA_DataStruct_Stock_min(res15).sort_index(), 'min')
        res30=get_indicator(QA_DataStruct_Stock_min(res30).sort_index(), 'min')
        res15.columns = [x.upper() + '_15M' for x in res15.columns]
        res30.columns = [x.upper() + '_30M' for x in res30.columns]
        sec_temp_data = [res15.join(res30).groupby('code').fillna(method='ffill')]
    else:
        sec_temp_data = []

    buy_list = target_list
    #QA_util_log_info('##buy_list ==================== {}'.format(buy_list), ui_log=None)
    print('code_select sec_temp_data')
    print(sec_temp_data)
    return(buy_list, sec_temp_data, source_data)


def signal(target_list, buy_list, position, sec_temp_data, day_temp_data, source_data, trading_date, mark_tm):
    QA_util_log_info(target_list)
    # 计算信号 提供基础信息 example
    # 输出1 signal 计划持有的code 目前此方案 1:表示持有 0:表示不持有
    # 输出2 signal 进出信号 signal 1:表示进场信号 0:表示无信号 -1:表示卖出信号

    # 提前执行部分
    # 盘前数据准备
    # 午盘数据准备
    if buy_list is None:
        buy_list = []

    if position is not None and position.shape[0] > 0:
        code_list = list(set(target_list + position.code.tolist()))
    else:
        code_list = target_list

    # check data time 在某时某刻之后获准获取数据
    while time_check_before(mark_tm):
        time.sleep(60)
        pass

    QA_util_log_info('##JOB Crawl Trading Data ==================== {}'.format(
        mark_tm), ui_log=None)
    QA_util_log_info('##JOB Code List ====================', ui_log=None)
    QA_util_log_info(code_list, ui_log=None)

    stm = trading_date + ' ' + mark_tm
    #try:
    data, data_15min = data_collect(code_list, trading_date, day_temp_data, sec_temp_data, source_data)

    #except:
    #    QA_util_log_info('##JOB Signal Failed ====================', ui_log=None)
    #    data = None

    #QA_util_log_info('##JOB 300910 ====================', ui_log=None)
    #QA_util_log_info(data[(data.code == '300910')&(data.date == trading_date)][['RRNG_15M','VAMP_JC','CLOSE_K','VAMPC_K','VAMP_K','DISTANCE','close','MIN_V_15M','camt_vol','signal','msg']]
    #                 )
    #QA_util_log_info(data[(data.code == '300910')&(data.date == trading_date)&(data.signal == 1)][['RRNG_15M','VAMP_JC','CLOSE_K','VAMPC_K','VAMP_K','DISTANCE','close','MIN_V_15M','camt_vol','signal','msg']]
    #                 )

    if data is not None:
        data = data.sort_index().loc[(stm),]


        QA_util_log_info('##JOB Finished Trading Signal ==================== {}'.format(
            mark_tm), ui_log=None)

        # add information
        # add name industry

        data.loc[data.code.isin([i for i in code_list if i not in target_list]) & (data.signal.isin([1])), 'signal'] = None
        #if len([i for i in position.code.tolist() if i not in buy_list]) > 0:

        QA_util_log_info('##IN_SIG DataFrame ====================', ui_log=None)
        #    data.loc[[i for i in position.code.tolist() if i not in buy_list]][data.signal == 1, ['signal']] = None
        QA_util_log_info(data[data.IN_SIG == 1][['IN_SIG','IN_PROB','OUT_SIG','OUT_PROB','signal','msg']], ui_log=None)

        QA_util_log_info('##OUT_SIG DataFrame ====================', ui_log=None)
        QA_util_log_info(data[data.OUT_SIG == 1][['IN_SIG','IN_PROB','OUT_SIG','OUT_PROB','signal','msg']], ui_log=None)

        #    data.loc[[i for i in position.code.tolist() if i not in buy_list]][data.signal == 1, ['signal']] = None
        QA_util_log_info('##Buy DataFrame ====================', ui_log=None)
        QA_util_log_info(data[data.signal == 1][['VAMP_K','CLOSE_K','VAMPC_K','DISTANCE','close',
                                                 'IN_SIG','IN_PROB','OUT_SIG','OUT_PROB','signal','msg']], ui_log=None)

        QA_util_log_info('##Sell DataFrame ====================', ui_log=None)
        QA_util_log_info(data[data.signal == 0][['VAMP_K','CLOSE_K','VAMPC_K','DISTANCE','close',
                                                 'IN_SIG','IN_PROB','OUT_SIG','OUT_PROB','signal','msg']], ui_log=None)

        # 方案2
        #data['signal'] = None
        #data.loc[data.SKDJ_CROSS2_HR == 1, "signal"] = 1
        #data.loc[data.SKDJ_CROSS1_HR == 1, "signal"] = -1
        #data.loc[data.SKDJ_TR_HR == 1, "signal"] = 0

        # msg
        return(data)
    else:
        return None


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
        print(data.code)
        #try:
        data =data.assign(industry=[i.TDX.values[0] if i is not None else None for i in [QA_fetch_stock_industryinfo(x) for x in data.code] ],
                          name=[i.values[0] if i is not None else None for i in [QA_fetch_stock_name(x) for x in data.code] ],)

        #data['industry'] = data.code.apply(lambda x:QA_fetch_stock_industryinfo(x).TDX.values[0])
        #data['name'] = data.code.apply(lambda x:QA_fetch_stock_name(x).values[0])
        print('industry')
        #except:
        #   data['industry'] = None
        #    data['name'] = None

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
    source_data = QA_fetch_get_stock_vwap(QA_util_get_pre_trade_date(trading_date,10), trading_date, code_list, type='1')
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