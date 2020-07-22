from QUANTAXIS.QAUtil import QA_util_get_last_day, QA_util_today_str, QA_util_log_info
from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_realtm_ask,QA_fetch_get_stock_realtm_bid,QA_fetch_get_stock_close
from QUANTTOOLS.account_manage.trading_message import send_trading_message
from QUANTTOOLS.account_manage.Client import get_Client,check_Client,get_UseCapital,get_AllCapital,get_StockPos
from QUANTTOOLS.account_manage.BUY import BUY
from QUANTTOOLS.account_manage.SELL import SELL
from QUANTTOOLS.account_manage.HOLD import HOLD
import pandas as pd
import time
import datetime
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
    sub_accounts= float(sub_accounts) - 10000

    if target is None:
        res = positions.set_index('证券代码')
        avg_account = 0
        res = res.assign(target=avg_account)
        res['目标持股数'] = 0
        res['测算持股金额'] = 0
    else:
        tar1 = target.reset_index().groupby('code').max()
        tar1['position'] = tar1.reset_index().groupby('code')['RANK'].count()

        sell_code = [i for i in list(positions.set_index('证券代码').index) if i not in list(tar1.index)]
        buy_code = [i for i in list(tar1.index) if i not in list(positions.set_index('证券代码').index)]
        hold_code = [i for i in list(tar1.index) if i in list(positions.set_index('证券代码').index)]

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

        res = pd.concat([sell_table, buy_table, hold_table], axis=0)

        res['股票余额'] = res['股票余额'].fillna(0)
        res['市值'] = res['市值'].fillna(0)
        res['position'] = res['position'].fillna(0)
        res['ask1'] = list(res.reset_index()['code'].apply(lambda x:QA_fetch_get_stock_realtm_ask(x)))
        res['bid1'] = list(res.reset_index()['code'].apply(lambda x:QA_fetch_get_stock_realtm_bid(x)))
        res['close'] = list(res.reset_index()['code'].apply(lambda x:QA_fetch_get_stock_close(x)))
        res['买卖价'] = res.apply(lambda x: func1(x['ask1'], x['bid1']),axis = 1)
        res['sort'] = res['买卖价'].rank(ascending=False)

        ##实时修正
        res.ix[res['ask1']==0,'position'] = 0
        avg_account = (sub_accounts * percent)/res['position'].sum()
        res = res.assign(target=avg_account)
        res['target'] = res['target'] * res['position']

        res['目标持股数'] = (res['target']/res['买卖价']/100).apply(lambda x: round(x, 0)*100)
        res['测算持股金额'] = res['目标持股数'] * res['买卖价']

        while res['测算持股金额'].sum() > sub_accounts:
            res.loc[list(res[res['sort'] == 1].index)]['目标持股数'] = res.loc[list(res[res['sort'] == 1].index)]['目标持股数'] - k
            res['测算持股金额'] = res['目标持股数'] * res['买卖价']
            k = k + 100

        #res['mark'] = res['tar'] - res['市值']
    res['deal'] = (res['目标持股数'] - res['股票余额'].apply(lambda x:float(x))).apply(lambda x:math.floor(x/100)*100)

    if Zbreak == True:
        res = res[(res.deal> 0) & (res.deal < 0)]
    return(res)

def trade_roboot(target_tar, account, trading_date, percent, strategy_id, type='end', exceptions = None):
    client = get_Client()
    sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)
    account_info = client.get_account(account)

    if target_tar is None:
        QA_util_log_info('触发清仓 ==================== {}'.format(trading_date), ui_log=None)
        e = send_trading_message(account, strategy_id, account_info, None, "触发清仓", None, 0, direction = 'SELL', type='MARKET', priceType=4,price=None, client=client)
    res = build(target_tar, positions, sub_accounts, percent)
    res1 = res

    client.cancel_all(account)
    while (res[res['deal']<0].shape[0] + res[res['deal']>0].shape[0]) > 0:

        h1 = int(datetime.datetime.now().strftime("%H"))
        if h1 >= 15 or h1 <= 9:
            QA_util_log_info('已过交易时段 ==================== {}'.format(trading_date), ui_log=None)
            send_actionnotice(strategy_id,'交易报告:{}'.format(trading_date),'已过交易时段',direction = 'HOLD',offset='HOLD',volume=None)
            break

        if res[res['deal']<0].shape[0] == 0:
            QA_util_log_info('无卖出动作 ==================== {}'.format(trading_date), ui_log=None)
        else:
            for code in res[res['deal'] < 0].index:
                target_pos = float(res.at[code, '目标持股数'])
                target = float(res.at[code, '股票余额'])
                name = res.at[code, 'NAME']
                industry = res.at[code, 'INDUSTRY']
                deal_pos = abs(float(res.at[code, 'deal']))
                close = float(res.at[code, 'close'])

                SELL(client, account, strategy_id, account_info,trading_date, code, name, industry, deal_pos, target_pos, target, close, type = type)

                time.sleep(3)

            time.sleep(15)

        if res[res['deal'] == 0].shape[0] == 0:
            QA_util_log_info('无持续持仓动作 ==================== {}'.format(trading_date), ui_log=None)
        else:
            for code in res[res['deal'] == 0].index:
                target_pos = float(res.at[code, '目标持股数'])
                target = float(res.at[code, '市值'])
                name = res.at[code, 'NAME']
                industry = res.at[code, 'INDUSTRY']
                close = float(res.at[code, 'close'])

                HOLD(strategy_id, account_info,trading_date, code, name, industry, target_pos, target)

                time.sleep(1)

        if res[res['deal'] > 0].shape[0] == 0:
            QA_util_log_info('无买入动作 ==================== {}'.format(trading_date), ui_log=None)
        else:
            for code in res[res['deal'] > 0].index:

                target_pos = float(res.at[code, '目标持股数'])
                target = float(res.at[code, '测算持股金额'])
                name = res.at[code, 'NAME']
                industry = res.at[code, 'INDUSTRY']
                deal_pos = abs(float(res.at[code, 'deal']))

                BUY(client, account, strategy_id, account_info,trading_date, code, name, industry, deal_pos, target_pos, target, close, type)

                time.sleep(3)

            time.sleep(10)

        if type == 'end':
            sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)
            sub_accounts = sub_accounts - frozen
            res = build(target_tar, positions, sub_accounts, percent, True)
        elif type == 'morning':
            break
        else:
            break
    QA_util_log_info('交易完成 ==================== {}'.format(trading_date), ui_log=None)
    send_actionnotice(strategy_id,'交易报告:{}'.format(trading_date),'交易完成',direction = 'HOLD',offset='HOLD',volume=None)

    return(res1)

if __name__ == '__main__':
    pass