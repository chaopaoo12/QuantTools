from QUANTAXIS.QAUtil import  QA_util_log_info
from QUANTTOOLS.QAStockETL.QAFetch.QATdx import QA_fetch_get_stock_realtime
import easyquotation
import pandas as pd
import math

def func1(x, y):
    if x == 0:
        return y
    else:
        return x

def floor_round(x):
    if isinstance(x, int):
        y = math.floor(x/100)*100
    else:
        y = x

    if y > x:
        return(x)
    else:
        return(y)

def merge_table(target, positions):

    if target is None:
        QA_util_log_info('##JOB Target is None', ui_log = None)
        target = pd.DataFrame({'NAME':None,'INDUSTRY':None,'Z_PROB':None,'O_PROB':None,'RANK':None,'position':None}).dropna()
    else:
        QA_util_log_info('##JOB Target is not None', ui_log = None)
        target = target.reset_index().groupby('code').max()
        target['position'] = target.reset_index().groupby('code')['RANK'].count()

    if positions is None:
        positions = pd.DataFrame({'证券名称':None, '股票余额':None, '可用余额':None, '冻结数量':None,
                                  '参考盈亏':None, '成本价':None, '市价':None, '市值':None,
                                  '盈亏比例(%)':None, '上市时间':None, 'INDUSTRY':None,
                                  'NAME':None}).dropna()
    else:
        positions =positions.set_index('code')

    QA_util_log_info('##JOB Separate Sell Buy Hold code', ui_log = None)
    sell_code = [i for i in list(positions.index) if i not in list(target.index)]
    buy_code = [i for i in list(target.index) if i not in list(positions.index)]
    hold_code = [i for i in list(target.index) if i in list(positions.index)]

    QA_util_log_info('##JOB Caculate Sell Buy Hold Frame', ui_log = None)
    if sell_code is not None and len(sell_code) > 0:
        sell_table = positions.loc[sell_code].join(target[[i for i in list(set(target.columns)) if i not in ['NAME', 'INDUSTRY']]],how='left')
    else:
        sell_table = pd.DataFrame()

    if buy_code is not None and len(buy_code) > 0:
        buy_table = target.loc[buy_code].join(positions[[i for i in list(set(positions.columns)) if i not in ['NAME', 'INDUSTRY']]],how='left')
    else:
        buy_table = pd.DataFrame()

    if hold_code is not None and len(hold_code) > 0:
        hold_table = target.loc[hold_code].join(positions[[i for i in list(set(positions.columns)) if i not in ['NAME', 'INDUSTRY']]],how='left')
    else:
        hold_table = pd.DataFrame()

    QA_util_log_info('##JOB Concat Sell Buy Hold Frame', ui_log = None)
    res = pd.concat([sell_table,
                     buy_table,
                     hold_table], axis=0)
    res['RANK'] = res['RANK'].fillna(0)
    res['股票余额'] = res['股票余额'].fillna(0)
    res['市值'] = res['市值'].fillna(0)

    res['可用余额'] = res['可用余额'].fillna(0)
    res['position'] = res['position'].fillna(0)
    QA_util_log_info(res)
    return(res)

def get_top(res, num = 5):
    QA_util_log_info('##JOB Add Info to Result Frame', ui_log = None)
    try:
        values = QA_fetch_get_stock_realtime(list(res.reset_index()['code']))[['ask1','bid1','close']]
        res = res.join(values)
    except:
        QA_util_log_info('##JOB Now Get RealTime Price Failed.')

    res['买卖价'] = res.apply(lambda x: func1(x['ask1'], x['bid1']),axis = 1)
    #可否加仓信号 1为可以加仓 0为否
    res['mark'] = res.ask1.apply(lambda x: 0 if x ==0 else 1)
    top_num = num
    QA_util_log_info(res[['NAME','INDUSTRY','close','mark','RANK','买卖价','ask1','bid1']])
    hold = res[(res.mark == 1) & (res.RANK > 0)].sort_values('RANK').head(top_num)
    res = res[(res['市值'] > 0) & (res.RANK == 0)].append(hold)
    return(res)

def caculate_position(res, percent, sub_accounts):

    QA_util_log_info('##Today Position {}'.format(percent), ui_log = None)
    ###初步资金分配
    if res['position'].sum() > 0:
        avg_account = (sub_accounts * percent)/res['position'].sum()
    else:
        avg_account = 0
    res = res.assign(target=avg_account)
    res['target'] = res['target'] * res['position']
    #总调仓金额确认
    #不可买入金额
    if res[(res['target'] > res['市值']) & (res['mark'] == 0)].shape[0] > 0:
        res['target_change'] = (res[(res['target'] > res['市值']) & (res['mark'] == 0)]['target'].fillna(0) - res[(res['target'] > res['市值']) & (res['mark'] == 0)]['市值'].fillna(0))
        change = res['target_change'].sum() / res[(res['target_change'] == 0) & (res['position'] > 0)].shape[0]
        res.loc[((res['target_change'] == 0) & (res['position'] > 0)),'target_change'] = change
    else:
        res['target_change'] = 0

    #check target_change.sum = 0
    if res['target_change'].sum() == 0:
        pass
    QA_util_log_info(res[['NAME','INDUSTRY','close','mark','target','target_change','position','股票余额','可用余额','冻结数量']])
    res['target'] = res['target'] * res['position'] + res['target_change']

    QA_util_log_info('##JOB Caculate Target Position', ui_log = None)
    res['买卖价'] = res.apply(lambda x: func1(x['ask1'], x['bid1']),axis = 1)
    res['目标持股数'] = res.apply(lambda x: math.floor(x['target'] / x['买卖价'] / 100)*100, axis=1)
    res['测算持股金额'] = res.apply(lambda x: x['目标持股数'] * x['买卖价'], axis=1)
    res['deal'] = res['目标持股数'] - res['股票余额']

    return(res)

def balance(res, k = 100):
    QA_util_log_info('##JOB Banlance Result', ui_log = None)
    ###流程
    #建立基础分配表 按均值计算取floor 正常情况下应该是仓位未用完
    #微调 加仓算法 缺省仓位 比较 目标持股单位操作金额
    res['sort_gp'] = res.target.apply(lambda x:1 if x == 0 else 0)
    res['price_rank'] = res.groupby('sort_gp')['买卖价'].rank(ascending = True)
    #求一个单位

    res['trim'] = 0
    QA_util_log_info(res[['NAME','mark','position','deal','target','测算持股金额','股票余额','可用余额','冻结数量']], ui_log = None)
    while (res['target'].sum() - res['测算持股金额'].sum()) > (res[res.RANK > 0]['买卖价'].min() * 100):
        QA_util_log_info('##JOB Banlance from {_from} to {_to} ADD Trim {trim}'.format(_from=res['target'].sum() - res['测算持股金额'].sum(),
                                                                                   _to=res[res.RANK > 0]['买卖价'].min() * 100,
                                                                                   trim=res.trim.max()), ui_log = None)
        ####调增判断
        ###调增
        #调整范围确认 行动为多买

        for i in range(len(list(res[res.sort_gp == 0].index)), 0, -1):
            QA_util_log_info(i)
            if res[(res.sort_gp == 0) & (res.price_rank <= i)]['买卖价'].apply(lambda x :x*100).sum() <= (res['target'].sum() - res['测算持股金额'].sum()):
                trim_code = list(res[(res.sort_gp == 0) & (res.price_rank <= i)].index)
                QA_util_log_info(trim_code)
                res.loc[trim_code,'trim'] = res.loc[trim_code,'trim'] + k
            else:
                pass
        res['目标持股数'] = res.apply(lambda x: x['目标持股数'] + x['trim'], axis=1)
        res['测算持股金额'] = res.apply(lambda x: x['目标持股数'] * x['买卖价'], axis=1)

    QA_util_log_info(res[['NAME','mark','position','deal','target','测算持股金额','股票余额','可用余额','冻结数量']], ui_log = None)
    while res['测算持股金额'].sum() > res['target'].sum():
        QA_util_log_info('##JOB Banlance from {_from} to {_to} DEC Trim {dec}'.format(_from=res['target'].sum(),
                                                                                      _to=res['测算持股金额'].sum(),
                                                                                      dec=res.trim.max()), ui_log = None)
        ####调减判断 行动为多卖
        for i in range(1, len(list(res[res.sort_gp == 0].index))+1, 1):
            if res[(res.sort_gp == 0) & (res.price_rank <= i)]['买卖价'].apply(lambda x :x*100).sum() > (res['测算持股金额'].sum() - res['target'].sum()):
                trim_code = list(res[(res.sort_gp == 0) & (res.price_rank <= i)].index)
                res.loc[trim_code,'trim'] = res.loc[trim_code,'trim'] - k
            else:
                pass
        res['目标持股数'] = res.apply(lambda x: x['目标持股数'] + x['trim'], axis=1)
        res['测算持股金额'] = res.apply(lambda x: x['目标持股数'] * x['买卖价'], axis=1)

    QA_util_log_info('##JOB Caculate Deal Position', ui_log = None)
    res['deal'] = res['目标持股数'] - res['股票余额']
    return(res)

def build(target, positions, sub_accounts, percent, k=100):
    QA_util_log_info('##JOB Now Check Sub Accounts', ui_log = None)
    sub_accounts = float(sub_accounts) - 10000

    res = merge_table(target, positions)

    if res is not None and res.shape[0] > 0:
        res = get_top(res , 5)

        QA_util_log_info('##JOB Refreash Result Frame', ui_log = None)
        #QA_util_log_info(res)
        if res is not None and res.shape[0] > 0:
            res = caculate_position(res, percent, sub_accounts)
            #QA_util_log_info(res[['NAME','INDUSTRY','deal','close','目标持股数','股票余额','可用余额','冻结数量','买卖价']])
            res = balance(res, k = k)
            QA_util_log_info('##JOB Dislodge Holding Position', ui_log = None)
            res = res[(res.deal> 0) | (res.deal < 0)]

    if res is None or res.shape[0] == 0:
        if res is None:
            QA_util_log_info('##JOB Target is None', ui_log = None)
            res = pd.DataFrame()

        for i in [i for i in ['NAME','INDUSTRY','deal','close','目标持股数','股票余额','可用余额','冻结数量'] if i not in list(res.columns)]:
            res[i] = None
        res = res.dropna()

    return(res)


if __name__ == 'main':
    pass