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

def build(target, positions, sub_accounts, percent, Zbreak, k=100):
    QA_util_log_info('##JOB Now Check Sub Accounts', ui_log = None)
    sub_accounts = float(sub_accounts) - 10000

    if target is None:
        QA_util_log_info('##JOB Target is None', ui_log = None)
        res = positions.set_index('code')
        res['position'] = 0
    else:
        QA_util_log_info('##JOB Target is not None', ui_log = None)
        tar1 = target.reset_index().groupby('code').max()
        tar1['position'] = tar1.reset_index().groupby('code')['RANK'].count()
        positions = positions.set_index('code')

        QA_util_log_info('##JOB Separate Sell Buy Hold code', ui_log = None)
        sell_code = [i for i in list(positions.index) if i not in list(tar1.index)]
        buy_code = [i for i in list(tar1.index) if i not in list(positions.index)]
        hold_code = [i for i in list(tar1.index) if i in list(positions.index)]

        QA_util_log_info('##JOB Caculate Sell Buy Hold Frame', ui_log = None)
        if sell_code is not None and len(sell_code) > 0:
            sell_table = positions.loc[sell_code].join(tar1[[i for i in list(set(tar1.columns)) if i not in ['NAME', 'INDUSTRY']]],how='left')
        else:
            sell_table = pd.DataFrame()

        if buy_code is not None and len(buy_code) > 0:
            buy_table = tar1.loc[buy_code].join(positions[[i for i in list(set(positions.columns)) if i not in ['NAME', 'INDUSTRY']]],how='left')
        else:
            buy_table = pd.DataFrame()

        if hold_code is not None and len(hold_code) > 0:
            hold_table = tar1.loc[hold_code].join(positions[[i for i in list(set(positions.columns)) if i not in ['NAME', 'INDUSTRY']]],how='left')
        else:
            hold_table = pd.DataFrame()

        QA_util_log_info('##JOB Concat Sell Buy Hold Frame', ui_log = None)
        res = pd.concat([sell_table,
                         buy_table,
                         hold_table], axis=0)

    QA_util_log_info('##JOB Add Info to Result Frame', ui_log = None)
    res['股票余额'] = res['股票余额'].fillna(0)
    res['市值'] = res['市值'].fillna(0)
    res['可用余额'] = res['可用余额'].fillna(0)
    res['position'] = res['position'].fillna(0)
    try:
        values = QA_fetch_get_stock_realtime(list(res.reset_index()['code']))[['ask1','bid1','close']]
        res = res.join(values)
    except:
        QA_util_log_info('##JOB Now Get RealTime Price Failed.')

    res['买卖价'] = res.apply(lambda x: func1(x['ask1'], x['bid1']),axis = 1)
    #可否加仓信号 1为可以加仓 0为否
    res['mark'] = res.ask1.apply(lambda x: 0 if x ==0 else 1)

    QA_util_log_info('##JOB Now Get Code with Top Price.')
    top_num = 5
    res[res['position'] > 0]
    res[res['市值'] > 0]
    res[(res['position'] > 0 & res['市值'] > 0)]
    stay_table = res[(res['position'] > 0 & res['市值'] > 0)]
    inc_table = res[(res['position'] > 0 & res['市值'] == 0 & res['mark'] == 1)].sort_values('RANK').head(top_num-stay_table.shape[0])
    res = stay_table.append(inc_table)

    ###初步资金分配
    QA_util_log_info('##JOB Refreash Result Frame', ui_log = None)
    QA_util_log_info('##Today Position {}'.format(percent), ui_log = None)
    if res['position'].sum() > 0:
        avg_account = (sub_accounts * percent)/res['position'].sum()
    else:
        avg_account = 0
    res = res.assign(target=avg_account)
    res['target'] = res['target'] * res['position']

    #总调仓金额确认
    #不可买入金额
    res['target_change'] = res[(res['target'] > res['市值'] & res['mark'] == 0)]['target'] - res[(res['target'] > res['市值'] & res['mark'] == 0)]['市值']
    change = res['target_change'].sum() / res[(res['target_change'] == 0 & res['position'] > 0)].shape[0]
    res.loc[(res['target_change'] == 0 & res['position'] > 0),'target_change'] = change

    #check target_change.sum = 0
    if res['target_change'].sum() == 0:
        pass

    res['target'] = res['target'] * res['position'] + res['target_change']

    QA_util_log_info('##JOB Caculate Target Position', ui_log = None)
    res['买卖价'] = res.apply(lambda x: func1(x['ask1'], x['bid1']),axis = 1)
    res['目标持股数'] = res.apply(lambda x: math.floor(x['target'] / x['买卖价'] / 100)*100, axis=1)
    res['测算持股金额'] = res.apply(lambda x: x['目标持股数'] * x['买卖价'], axis=1)
    res['deal'] = res['目标持股数'] - res['股票余额']
    #QA_util_log_info(res[['NAME','INDUSTRY','deal','close','target','position','目标持股数','股票余额','可用余额','冻结数量','买卖价']])
    #res['deal'] = res.apply(lambda x: x['deal'] if -x['deal'] < x['可用余额'] else -x['可用余额'], axis = 1)
    QA_util_log_info(res[['NAME','INDUSTRY','deal','close','目标持股数','股票余额','可用余额','冻结数量','买卖价']])

    QA_util_log_info('##JOB Refresh Final Result', ui_log = None)
    ###流程
    #建立基础分配表 按均值计算取floor 正常情况下应该是仓位未用完
    #微调 加仓算法 缺省仓位 比较 目标持股单位操作金额
    res['sort_gp'] = res.target.apply(lambda x:1 if x == 0 else 0)
    res['price_rank'] = res.groupby('sort_gp')['买卖价'].rank(ascending = True)
    #求一个单位

    k = 100
    res['trim'] = 0

    while (res['测算持股金额'].sum() - res['target'].sum()) < -10000:
        ####调增判断
        ###调增
        #调整范围确认 行动为多买
        for i in range(len(list(res[res.sort_gp == 0].index)), 0, -1):
            if res[(res.sort_gp == 0 & res.price_rank <= i)]['买卖价'].apply(lambda x :x*100).sum() <= (res['测算持股金额'].sum() - res['target'].sum()):
                trim_code = list(res[(res.sort_gp == 0 & res.price_rank <= i)].index)
                res.loc[trim_code,'trim'] = res.loc[trim_code,'trim'] + k
            else:
                pass
        res['目标持股数'] = res.apply(lambda x: x['目标持股数'] + x['trim'], axis=1)
        res['测算持股金额'] = res.apply(lambda x: x['目标持股数'] * x['买卖价'], axis=1)

    while res['测算持股金额'].sum() > res['target'].sum():
        ####调减判断 行动为多卖
        for i in range(1, len(list(res[res.sort_gp == 0].index))+1, 1):
            if res[(res.sort_gp == 0 & res.price_rank <= i)]['买卖价'].apply(lambda x :x*100).sum() > (res['测算持股金额'].sum() - res['target'].sum()):
                trim_code = list(res[(res.sort_gp == 0 & res.price_rank <= i)].index)
                res.loc[trim_code,'trim'] = res.loc[trim_code,'trim'] - k
            else:
                pass
        res['目标持股数'] = res.apply(lambda x: x['目标持股数'] + x['trim'], axis=1)
        res['测算持股金额'] = res.apply(lambda x: x['目标持股数'] * x['买卖价'], axis=1)

    QA_util_log_info('##JOB Caculate Deal Position', ui_log = None)
    res['deal'] = res['目标持股数'] - res['股票余额']
    #res['deal'] = res.apply(lambda x: x['deal'] if -x['deal'] <= x['可用余额'] else -x['可用余额'], axis = 1)

    if Zbreak == True:
        QA_util_log_info('##JOB Stop Confirm', ui_log = None)

        if res[res.target == 0]['市值'].sum() == 0 and \
                (res['目标持股数'] - res['股票余额']).apply(lambda x:abs(x)).sum() <= 5000:
            res = res.assign(deal=0)
        else:
            QA_util_log_info('##JOB Dislodge Holding Position', ui_log = None)
            res = res[(res.deal> 0) | (res.deal < 0)]
    return(res)

if __name__ == 'main':
    pass