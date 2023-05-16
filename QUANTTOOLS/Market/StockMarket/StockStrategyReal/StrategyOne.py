from QUANTTOOLS.Market.MarketTools.TimeTools.time_control import time_check_before,time_check_after
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_vwap
from QUANTAXIS.QAUtil import QA_util_log_info, QA_util_get_pre_trade_date
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_name,QA_fetch_stock_industryinfo, QA_fetch_get_stock_realtime
import time
from QUANTTOOLS.QAStockETL.QAData import QA_DataStruct_Stock_min
from QUANTAXIS.QAFetch.QAQuery_Advance import QA_fetch_stock_min_adv
from QUANTAXIS import QA_fetch_get_index_realtime
from QUANTTOOLS.QAStockETL.QAFetch.QAIndicator import get_indicator_real
import pandas as pd

def data_base(code_list,trading_date,proxies):
    data = QA_fetch_get_stock_vwap(code_list, QA_util_get_pre_trade_date(trading_date,10), trading_date,
                                   period = '1', type = 'sina',proxies=proxies)
    return(data)

def data_collect(code_list, trading_date, day_temp_data, sec_temp_data, source_data, position, mark_tm, proxies):
    #try:
    source_data = QA_fetch_get_stock_realtime(source='qq',code=code_list)
    source_data = source_data.reset_index()
    ##
    source_data = source_data.assign(datetime = pd.to_datetime(mark_tm)).set_index(
        ['datetime', 'code'])[['last_close','price','open','high','low','ask1','ask2','bid1']].sort_index()

    data = source_data.join(sec_temp_data[0])
    #data = data.loc[mark_tm]

    data = data.reset_index().set_index('code').join(position.set_index('code')).reset_index().set_index(['datetime','code'])

    data['signal'] = None
    data['msg'] = None
    data = data.assign(signal = None,
                       msg = None,
                       code = [str(i) for i in data.reset_index().code])
    QA_util_log_info('##JOB Out Signal Decide ====================', ui_log=None)

    data.loc[(data.price > data.UB_15M_V * 1.05)&
             ((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0)), "signal"] = 0
    data.loc[(data.price > data.UB_15M_V * 1.05)&
             ((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0)), "msg"] = '15min 超涨止盈信号'

    data.loc[(data.price > data.UB_30M_V * 1.05)&
             ((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0)), "signal"] = 0
    data.loc[(data.price > data.UB_30M_V * 1.05)&
             ((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0)), "msg"] = '30min 超涨止盈信号'

    data.loc[(data.price >= data.UB_15M_V * 0.99)&
             (((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0))|
              ((data.BOLL_5M_S2 > 0)&(data.BOLL_5M_S > 0)&(data.BOLL_5M < 0))), "signal"] = 0
    data.loc[(data.price >= data.UB_15M_V * 0.99)&
             (((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0))|
              ((data.BOLL_5M_S2 > 0)&(data.BOLL_5M_S > 0)&(data.BOLL_5M < 0))), "msg"] = '15Min 触顶出场信号'

    data.loc[(data.price >= data.UB_30M_V * 0.99)&
             (((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0))|
              ((data.BOLL_5M_S2 > 0)&(data.BOLL_5M_S > 0)&(data.BOLL_5M < 0))), "signal"] = 0
    data.loc[(data.price >= data.UB_30M_V * 0.99)&
             (((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0))|
              ((data.BOLL_5M_S2 > 0)&(data.BOLL_5M_S > 0)&(data.BOLL_5M < 0))), "msg"] = '30Min 触顶出场信号'

    data.loc[(data.price >= data.BOLL_15M_V * 0.99)&(data.price <= data.BOLL_15M_V * 1.01)&
             (((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0))|
              ((data.BOLL_5M_S2 > 0)&(data.BOLL_5M_S > 0)&(data.BOLL_5M < 0))), "signal"] = 0
    data.loc[(data.price >= data.BOLL_15M_V * 0.99)&(data.price <= data.BOLL_15M_V * 1.01)&
             (((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0))|
              ((data.BOLL_5M_S2 > 0)&(data.BOLL_5M_S > 0)&(data.BOLL_5M < 0))), "msg"] = '15Min BOLL触顶出场信号'

    data.loc[(data.price >= data.BOLL_30M_V * 0.99)&(data.price <= data.BOLL_30M_V * 1.01)&
             (((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0))|
              ((data.BOLL_5M_S2 > 0)&(data.BOLL_5M_S > 0)&(data.BOLL_5M < 0))), "signal"] = 0
    data.loc[(data.price >= data.BOLL_30M_V * 0.99)&(data.price <= data.BOLL_30M_V * 1.01)&
             (((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0))|
              ((data.BOLL_5M_S2 > 0)&(data.BOLL_5M_S > 0)&(data.BOLL_5M < 0))), "msg"] = '30Min BOLL触顶出场信号'

    data.loc[(data.price <= data.LB_15M_V)&
             (((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0))|
              ((data.BOLL_5M_S2 > 0)&(data.BOLL_5M_S > 0)&(data.BOLL_5M < 0))), "signal"] = 0
    data.loc[(data.price <= data.LB_15M_V)&
             (((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0))|
              ((data.BOLL_5M_S2 > 0)&(data.BOLL_5M_S > 0)&(data.BOLL_5M < 0))), "msg"] = '15Min LB止损出场信号'

    data.loc[(data.price <= data.LB_30M_V)&
             (((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0))|
              ((data.BOLL_5M_S2 > 0)&(data.BOLL_5M_S > 0)&(data.BOLL_5M < 0))), "signal"] = 0
    data.loc[(data.price <= data.LB_30M_V)&
             (((data.UB_5M_S2 > 0)&(data.UB_5M_S > 0)&(data.UB_5M < 0))|
              ((data.BOLL_5M_S2 > 0)&(data.BOLL_5M_S > 0)&(data.BOLL_5M < 0))), "msg"] = '30Min LB止损出场信号'

    data.loc[(data.price <= data.BOLL_5M_V)&(data.BOLL_5M < 0), "signal"] = 0
    data.loc[(data.price <= data.BOLL_5M_V)&(data.BOLL_5M < 0), "msg"] = '5Min BOLL止损出场信号'

    # 强制止损
    data.loc[data['盈亏比例(%)'] <= -5, "signal"] = 0
    data.loc[data['盈亏比例(%)'] <= -5, "msg"] = '强制止损'

    QA_util_log_info('##JOB In Signal Decide ====================', ui_log=None)
    # 放量金叉
    data.loc[(data.price < data.LB_15M_V * 1.01)&
             ((data.UB_5M_S2 < 0)&(data.UB_5M_S < 0)&(data.UB_5M > 0)), "signal"] = 1
    data.loc[(data.price < data.LB_15M_V * 1.01)&
              ((data.UB_5M_S2 < 0)&(data.UB_5M_S < 0)&(data.UB_5M > 0)), "msg"] = '15min LB进场信号'

    data.loc[(data.price <= data.LB_30M_V * 0.95)&(data.price <= data.LB_15M_V * 0.95)&
             (((data.BOLL_5M_S2 < 0)&(data.BOLL_5M_S < 0)&(data.BOLL_5M > 0)) |
              ((data.LB_5M_S2 < 0)&(data.LB_5M_S < 0)&(data.LB_5M > 0))), "signal"] = 1
    data.loc[(data.price <= data.LB_30M_V * 0.95)&(data.price <= data.LB_15M_V * 0.95)&
             (((data.BOLL_5M_S2 < 0)&(data.BOLL_5M_S < 0)&(data.BOLL_5M > 0)) |
              ((data.LB_5M_S2 < 0)&(data.LB_5M_S < 0)&(data.LB_5M > 0))), "msg"] = 'BOLL LB抄底进场信号'

    data.loc[(data.price <= data.LB_30M_V * 1.01)&
              ((data.UB_5M_S2 < 0)&(data.UB_5M_S < 0)&(data.UB_5M > 0)), "signal"] = 1
    data.loc[(data.price <= data.LB_30M_V * 1.01)&
              ((data.UB_5M_S2 < 0)&(data.UB_5M_S < 0)&(data.UB_5M > 0)), "msg"] = '30Min LB进场信号'

    #data.loc[(data.price <= data.BOLL_15M_V * 1.01)&(data.price >= data.BOLL_15M_V * 0.99)&
    #         (((data.UB_5M_S2 < 0)&(data.UB_5M_S < 0)&(data.UB_5M > 0))), "signal"] = 1
    #data.loc[(data.price <= data.BOLL_15M_V * 1.01)&(data.price >= data.BOLL_15M_V * 0.99)&
    #         (((data.UB_5M_S2 < 0)&(data.UB_5M_S < 0)&(data.UB_5M > 0))), "msg"] = '15Min BOLL进场信号'

    data.loc[(data.price <= data.BOLL_30M_V * 1.01)&(data.price >= data.BOLL_30M_V * 0.99)&
              ((data.UB_5M_S2 < 0)&(data.UB_5M_S < 0)&(data.UB_5M > 0)), "signal"] = 1
    data.loc[(data.price <= data.BOLL_30M_V * 1.01)&(data.price >= data.BOLL_30M_V * 0.99)&
              ((data.UB_5M_S2 < 0)&(data.UB_5M_S < 0)&(data.UB_5M > 0)), "msg"] = '30Min BOLL进场信号'


    QA_util_log_info('##IN_SIG DataFrame ====================', ui_log=None)
    QA_util_log_info(data[data.signal == 1][['price','LB_15M_V','BOLL_15M_V','LB_30M_V','BOLL_30M_V','signal','msg']], ui_log=None)

    QA_util_log_info('##OUT_SIG DataFrame ====================', ui_log=None)
    QA_util_log_info(data[data.signal == 0][['price','UB_15M_V','BOLL_15M_V','UB_30M_V','BOLL_30M_V','signal','msg']], ui_log=None)

    return(data, [sec_temp_data])

def day_init(target_list, trading_date):

    QA_util_log_info('##JOB Init Day Data ==================== {}'.format(
        trading_date), ui_log=None)
    his5_data= QA_fetch_stock_min_adv(target_list,QA_util_get_pre_trade_date(trading_date,10),
                                       QA_util_get_pre_trade_date(trading_date,5),'5min')
    his15_data= QA_fetch_stock_min_adv(target_list,QA_util_get_pre_trade_date(trading_date,10),
                                       QA_util_get_pre_trade_date(trading_date,5),'15min')
    his30_data= QA_fetch_stock_min_adv(target_list,QA_util_get_pre_trade_date(trading_date,10),
                                       QA_util_get_pre_trade_date(trading_date,5),'30min')

    return([his15_data, his30_data, his5_data])

def code_select(target_list, position, day_temp_data, sec_temp_data, trading_date, mark_tm, proxies):

    if target_list is None:
        target_list = []

    if position is not None and position.shape[0] > 0:
        code_list = target_list + position.code.tolist()
    else:
        code_list = target_list

    code_list = list(set(code_list))
    QA_util_log_info('##JOB Refresh Code List ==================== {}'.format(
        mark_tm), ui_log=None)

    source_data = data_base(code_list, trading_date, proxies)

    temp = source_data.assign(type='1min')
    temp = QA_DataStruct_Stock_min(temp)
    res5=pd.concat([temp.min5, day_temp_data[2].data])
    res15=pd.concat([temp.min15, day_temp_data[0].data])
    res30=pd.concat([temp.min30, day_temp_data[1].data])
    res5 = res5[~res5.index.duplicated(keep='first')]
    res15 = res15[~res15.index.duplicated(keep='first')]
    res30 = res30[~res30.index.duplicated(keep='first')]
    res5=get_indicator_real(QA_DataStruct_Stock_min(res5.sort_index()), 'min', keep = True)
    res15=get_indicator_real(QA_DataStruct_Stock_min(res15.sort_index()), 'min', keep = True)
    res30=get_indicator_real(QA_DataStruct_Stock_min(res30.sort_index()), 'min', keep = True)
    res5.columns = [x.upper() + '_5M' for x in res5.columns]
    res15.columns = [x.upper() + '_15M' for x in res15.columns]
    res30.columns = [x.upper() + '_30M' for x in res30.columns]
    res5[['UB_5M_S','BOLL_5M_S','LB_5M_S']] = res5.groupby('code')[['UB_5M','BOLL_5M','LB_5M']].shift()
    res5[['UB_5M_S2','BOLL_5M_S2','LB_5M_S2']] = res5.groupby('code')[['UB_5M','BOLL_5M','LB_5M']].shift(2)
    res15[['UB_15M_S','BOLL_15M_S','LB_15M_S']] = res15.groupby('code')[['UB_15M','BOLL_15M','LB_15M']].shift()
    res15[['UB_15M_S2','BOLL_15M_S2','LB_15M_S2']] = res15.groupby('code')[['UB_15M','BOLL_15M','LB_15M']].shift(2)
    res30[['UB_30M_S','BOLL_30M_S','LB_30M_S']] = res30.groupby('code')[['UB_30M','BOLL_30M','LB_30M']].shift()
    res30[['UB_30M_S2','BOLL_30M_S2','LB_30M_S2']] = res30.groupby('code')[['UB_30M','BOLL_30M','LB_30M']].shift(2)

    sec_temp_data = [res5.join(res15).join(res30).groupby('code').fillna(method='ffill')]
    sec_temp_data = [sec_temp_data[0].assign(BOLL_5M_V = sec_temp_data[0].CLOSE_5M / (sec_temp_data[0].BOLL_5M + 1),
                                             LB_5M_V = sec_temp_data[0].CLOSE_5M / (sec_temp_data[0].LB_5M + 1),
                                             UB_5M_V = sec_temp_data[0].CLOSE_5M / (sec_temp_data[0].UB_5M + 1),
                                             BOLL_15M_V = sec_temp_data[0].CLOSE_15M / (sec_temp_data[0].BOLL_15M + 1),
                                            LB_15M_V = sec_temp_data[0].CLOSE_15M / (sec_temp_data[0].LB_15M + 1),
                                            UB_15M_V = sec_temp_data[0].CLOSE_15M / (sec_temp_data[0].UB_15M + 1),
                                            BOLL_30M_V = sec_temp_data[0].CLOSE_30M / (sec_temp_data[0].BOLL_30M + 1),
                                            LB_30M_V = sec_temp_data[0].CLOSE_30M / (sec_temp_data[0].LB_30M + 1),
                                            UB_30M_V = sec_temp_data[0].CLOSE_30M / (sec_temp_data[0].UB_30M + 1)
                                            )]

    buy_list = list(set(sec_temp_data[0][sec_temp_data[0].WIDTH_30M >= 0.15].reset_index().code.tolist()))

    QA_util_log_info('##buy_list ==================== {}'.format(len(buy_list)), ui_log=None)
    return(buy_list, sec_temp_data, source_data)


def signal(target_list, buy_list, position, sec_temp_data, day_temp_data, source_data, trading_date, mark_tm, proxies):
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
    data, data_15min = data_collect(code_list, trading_date, day_temp_data, sec_temp_data, source_data, position, stm, proxies)

    if data is not None:
        data = data.sort_index().loc[(stm),]


        QA_util_log_info('##JOB Finished Trading Signal ==================== {}'.format(
            mark_tm), ui_log=None)

        # add information
        # add name industry
        QA_util_log_info('##Lowe ==================== {}'.format(
            mark_tm), ui_log=None)
        QA_util_log_info(data[(data.price < data.LB_15M_V* 1.01)|(data.price < data.LB_30M_V* 1.01)][['price','盈亏比例(%)','signal','msg']], ui_log=None)
        QA_util_log_info('##Up ==================== {}'.format(mark_tm), ui_log=None)
        QA_util_log_info(data[(data.price > data.UB_15M_V* 0.99)|(data.price < data.UB_30M_V* 0.99)][['price','盈亏比例(%)','signal','msg']], ui_log=None)
        QA_util_log_info('##15 mid ==================== {}'.format(mark_tm), ui_log=None)
        QA_util_log_info(data[(data.price <= data.BOLL_15M_V * 1.01)&(data.price >= data.BOLL_15M_V * 0.99)][['price','盈亏比例(%)','signal','msg']], ui_log=None)
        QA_util_log_info('##30 mid ==================== {}'.format(mark_tm), ui_log=None)
        QA_util_log_info(data[(data.price <= data.BOLL_30M_V * 1.01)&(data.price >= data.BOLL_30M_V * 0.99)][['price','盈亏比例(%)','signal','msg']], ui_log=None)
        data.loc[data.code.isin([i for i in code_list if i not in target_list]) & (data.signal.isin([1])), 'signal'] = None
        QA_util_log_info('##Buy DataFrame ====================', ui_log=None)
        QA_util_log_info(data[data.signal == 1][['price','LB_15M_V','BOLL_15M_V','LB_30M_V','BOLL_30M_V','盈亏比例(%)','signal','msg']], ui_log=None)

        #if position is not None:
        #    hold = position.shape[0]
        #else:
        #    hold = 0

        #if data[data.signal == 1].shape[0] > 0 and hold > 1:
        #    data.loc[data.code.isin([i for i in code_list if i not in buy_list]) & (data.signal.isnull()), 'signal'] = 0
        #    data.loc[data.code.isin([i for i in code_list if i not in buy_list]) & (data.signal.isnull()), 'msg'] = '换仓'

        QA_util_log_info('##Sell DataFrame ====================', ui_log=None)
        QA_util_log_info(data[data.signal == 0][['price','UB_15M_V','BOLL_15M_V','UB_30M_V','BOLL_30M_V','盈亏比例(%)','signal','msg']], ui_log=None)

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
            pass
        else:
            data = data[(data.signal.isin([0, 1]))]
            data = data.assign(市值=0, 可用余额=0)

        data = data.assign(target_position = data.signal)
        # 1 / data.signal.sum())
        data = data.assign(target_capital=data.target_position * sub_account * percent)
        data = data[data.target_capital.notnull()|(data['市值'] > 0)|data.signal.notnull()]
        QA_util_log_info(data[['signal','msg','target_capital','市值']], ui_log=None)
        # 方案2
        # data = pd.assign(target_position=1 / data.signal.sum(),
        #                 target_capital=data.target_position * sub_account * percent)
        #try:
        data =data.assign(industry=[i.TDX.values[0] if i is not None else None for i in [QA_fetch_stock_industryinfo(x) for x in data.code] ],
                          name=[i.values[0] if i is not None else None for i in [QA_fetch_stock_name(x) for x in data.code] ],)

        #data['industry'] = data.code.apply(lambda x:QA_fetch_stock_industryinfo(x).TDX.values[0])
        #data['name'] = data.code.apply(lambda x:QA_fetch_stock_name(x).values[0])
        #except:
        #   data['industry'] = None
        #    data['name'] = None

        data['mark'] = None
        data[["市值"]] = data[["市值"]].fillna(0)
        data.loc[(data["target_capital"] > data["市值"]) & (data.signal == 1), "mark"] = "buy"
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