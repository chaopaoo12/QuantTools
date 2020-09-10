from QUANTAXIS.QAUtil import  QA_util_log_info
from QUANTAXIS import QA_fetch_get_stock_realtime
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
        avg_account = 0
        res = res.assign(target=avg_account)
        res['目标持股数'] = 0
        res['测算持股金额'] = 0
        tar1 = target
        sell_code = list(res.index)
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
            quotation = easyquotation.use('sina')
            values = pd.DataFrame(quotation.stocks(list(res.reset_index()['code']))).T[['ask1','bid1','close']]
            print(values.index)
            print(res.index)
            res = res.join(values)
        except:
            QA_util_log_info('##JOB Now Get RealTime Price Failed.')
        res['买卖价'] = res.apply(lambda x: func1(x['ask1'], x['bid1']),axis = 1)
        QA_util_log_info(res[res['买卖价'] == 0])
        res = res[res['买卖价'] > 0]
        res['sort_gp'] = 1
        res.loc[sell_code,'sort_gp']=0
        res['sort'] = res.groupby('sort_gp')['买卖价'].rank(ascending = True)
        res.loc[res.sort_gp == 0, 'sort']=0

        QA_util_log_info('##JOB Refreash Result Frame', ui_log = None)
        ##实时修正
        res.loc[res.ask1 == 0,'position'] = 0
        avg_account = (sub_accounts * percent)/res['position'].sum()
        res = res.assign(target=avg_account)
        res['target'] = res['target'] * res['position']

        QA_util_log_info('##JOB Caculate Target Position', ui_log = None)
        res['目标持股数'] = res.apply(lambda x: math.floor(x['target'] / x['买卖价'] / 100)*100, axis=1)
        res['测算持股金额'] = res.apply(lambda x: x['目标持股数'] * x['买卖价'], axis=1)

        QA_util_log_info('##JOB Refresh Final Result', ui_log = None)
        k = 100
        while (res['测算持股金额'].sum() - (sub_accounts * percent)) <= 10000:
            QA_util_log_info('##JOB Budget {budget} Less than Capital {capital} k: {k}'.format(k=k,
                                                                                               budget=res['测算持股金额'].sum(),
                                                                                               capital = (sub_accounts * percent)), ui_log = None)
            res['trim'] = list(res['sort'].apply(lambda x:k if x == 1 else 0))
            res['目标持股数'] = res.apply(lambda x: x['目标持股数'] + x['trim'], axis=1)
            res['测算持股金额'] = res.apply(lambda x: x['目标持股数'] * x['买卖价'], axis=1)

        while res['测算持股金额'].sum() > (sub_accounts * percent):
            QA_util_log_info('##JOB Budget {budget} Larger than Capital {capital} k: {k}'.format(k=k,
                                                                             budget=res['测算持股金额'].sum(),
                                                                             capital = (sub_accounts * percent)), ui_log = None)
            res['trim'] = list(res['sort'].apply(lambda x:k if x == 1 else 0))
            res['目标持股数'] = res.apply(lambda x: x['目标持股数'] - x['trim'], axis=1)
            res['测算持股金额'] = res.apply(lambda x: x['目标持股数'] * x['买卖价'], axis=1)

    QA_util_log_info('##JOB Caculate Deal Position', ui_log = None)
    res['deal'] = (res['目标持股数'] - res['股票余额'].apply(lambda x:float(x))).apply(lambda x:math.floor(x/100)*100)
    res['deal'] = res.apply(lambda x: x['deal'] if -x['deal'] <= x['可用余额'] else -x['可用余额'], axis = 1)

    if Zbreak == True:
        QA_util_log_info('##JOB Stop Confirm', ui_log = None)
        if res.loc[sell_code]['市值'].sum() == 0 and abs(res.loc[list(tar1.index)]['市值'].sum() - res.loc[list(tar1.index)]['target'].sum()) <= 5000:
            res = res.assign(deal=0)
        else:
            QA_util_log_info('##JOB Dislodge Holding Position', ui_log = None)
            res = res[(res.deal> 0) | (res.deal < 0)]
    return(res)

if __name__ == 'main':
    pass