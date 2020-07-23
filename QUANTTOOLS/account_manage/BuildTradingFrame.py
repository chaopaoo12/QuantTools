from QUANTAXIS.QAUtil import  QA_util_log_info
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_realtm_ask,QA_fetch_get_stock_realtm_bid,QA_fetch_get_stock_close
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
    sub_accounts= float(sub_accounts) - 10000

    if target is None:
        QA_util_log_info('##JOB Target is None', ui_log = None)
        res = positions.set_index('证券代码')
        avg_account = 0
        res = res.assign(target=avg_account)
        res['目标持股数'] = 0
        res['测算持股金额'] = 0
    else:
        QA_util_log_info('##JOB Target is not None', ui_log = None)
        tar1 = target.reset_index().groupby('code').max()
        tar1['position'] = tar1.reset_index().groupby('code')['RANK'].count()

        QA_util_log_info('##JOB Separate Sell Buy Hold code', ui_log = None)
        sell_code = [i for i in list(positions.set_index('证券代码').index) if i not in list(tar1.index)]
        buy_code = [i for i in list(tar1.index) if i not in list(positions.set_index('证券代码').index)]
        hold_code = [i for i in list(tar1.index) if i in list(positions.set_index('证券代码').index)]

        QA_util_log_info('##JOB Caculate Sell Buy Hold Frame', ui_log = None)
        if sell_code is not None and len(sell_code) > 0:
            sell_table = positions.loc[sell_code].join(tar1[[i for i in list(positions.columns) if i not in ['NAME', 'INDUSTRY']]])
        else:
            sell_table = pd.DataFrame()

        if buy_code is not None and len(buy_code) > 0:
            buy_table = tar1.loc[buy_code].join(positions[[i for i in list(positions.columns) if i not in ['NAME', 'INDUSTRY']]].set_index('证券代码'))
        else:
            buy_table = pd.DataFrame()

        if hold_code is not None and len(hold_code) > 0:
            hold_table = tar1.loc[hold_code].join(positions[[i for i in list(positions.columns) if i not in ['NAME', 'INDUSTRY']]].set_index('证券代码'))
        else:
            hold_table = pd.DataFrame()

        QA_util_log_info('##JOB Concat Sell Buy Hold Frame', ui_log = None)
        res = pd.concat([sell_table, buy_table, hold_table], axis=0)

        QA_util_log_info('##JOB Add Info to Result Frame', ui_log = None)
        res['股票余额'] = res['股票余额'].fillna(0)
        res['市值'] = res['市值'].fillna(0)
        res['position'] = res['position'].fillna(0)
        res['ask1'] = list(res.reset_index()['code'].apply(lambda x:QA_fetch_get_stock_realtm_ask(x)))
        res['bid1'] = list(res.reset_index()['code'].apply(lambda x:QA_fetch_get_stock_realtm_bid(x)))
        res['close'] = list(res.reset_index()['code'].apply(lambda x:QA_fetch_get_stock_close(x)))
        res['买卖价'] = res.apply(lambda x: func1(x['ask1'], x['bid1']),axis = 1)
        res['sort'] = res['买卖价'].rank(ascending=False)

        QA_util_log_info('##JOB Refreash Result Frame', ui_log = None)
        ##实时修正
        res.ix[res['ask1']==0,'position'] = 0
        avg_account = (sub_accounts * percent)/res['position'].sum()
        res = res.assign(target=avg_account)
        res['target'] = res['target'] * res['position']

        QA_util_log_info('##JOB Caculate Target Position', ui_log = None)
        res['目标持股数'] = (res['target']/res['买卖价']/100).apply(lambda x: round(x, 0)*100)
        res['测算持股金额'] = res['目标持股数'] * res['买卖价']

        QA_util_log_info('##JOB Refresh Final Result', ui_log = None)
        while res['测算持股金额'].sum() > sub_accounts:
            QA_util_log_info('##JOB Budget Larger than Capital', ui_log = None)
            res['trim'] = list(res['sort'].apply(lambda x:k if x == 1 else 0))
            res['目标持股数'] = res['目标持股数'] - res['trim']
            #res.loc[list(res[res['sort'] == 1].index)]['目标持股数'] = res.loc[list(res[res['sort'] == 1].index)]['目标持股数'] - k
            res['测算持股金额'] = res['目标持股数'] * res['买卖价']
            print('k',k)
            k = k + 100

        #res['mark'] = res['tar'] - res['市值']
    QA_util_log_info('##JOB Caculate Deal Position', ui_log = None)
    res['deal'] = (res['目标持股数'] - res['股票余额'].apply(lambda x:float(x))).apply(lambda x:math.floor(x/100)*100)

    if Zbreak == True:
        QA_util_log_info('##JOB Dislodge Holding Position', ui_log = None)
        res = res[(res.deal> 0) & (res.deal < 0)]
    return(res)