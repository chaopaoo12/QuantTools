from QUANTAXIS.QAUtil import QA_util_get_last_day, QA_util_today_str, QA_util_log_info
from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_realtm_ask,QA_fetch_get_stock_realtm_bid,QA_fetch_get_stock_close
from QUANTTOOLS.account_manage.trading_message import send_trading_message
from QUANTTOOLS.account_manage.Client import get_Client,check_Client,get_UseCapital,get_AllCapital,get_StockPos
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

def re_build(target, positions, sub_accounts, percent, Zbreak, k=100):
    sub_accounts= float(sub_accounts) - 10000

    if target is None:
        res = positions.set_index('证券代码')
        avg_account = 0
        res = res.assign(tar=avg_account)
        res['cnt'] = 0
        res['real'] = 0
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
        res = res.assign(tar=avg_account)
        res['目标持仓金额'] = res['目标持仓金额'] * res['position']

        res['目标持股数'] = (res['tar']/res['买卖价']/100).apply(lambda x: round(x, 0)*100)
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

def build(target, positions, sub_accounts, percent, Zbreak=False, k=100):
    res = re_build(target, positions, sub_accounts, percent, Zbreak,k=k)
    while res['tar'].sum() < res['real'].sum():
        k = k + 100
        res = re_build(target, positions, sub_accounts, percent, k=k)
    return(res)

def trade_roboot(target, account, trading_date, percent, strategy_id, type='end', exceptions = None):
    client = get_Client()
    sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)
    account_info = client.get_account(account)

    if target is None:
        QA_util_log_info('触发清仓 ==================== {}'.format(trading_date), ui_log=None)
        e = send_trading_message(account, strategy_id, account_info, None, "触发清仓", None, 0, direction = 'SELL', type='MARKET', priceType=4,price=None, client=client)
    res = build(target, positions, sub_accounts, percent)
    res1 = res

    client.cancel_all(account)
    while (res[res['mark']<0].shape[0] + res[res['mark']>0].shape[0]) > 0:

        h1 = int(datetime.datetime.now().strftime("%H"))
        if h1 >= 15 or h1 <= 9:
            QA_util_log_info('已过交易时段 ==================== {}'.format(trading_date), ui_log=None)
            send_actionnotice(strategy_id,
                              '交易报告:{}'.format(trading_date),
                              '已过交易时段',
                              direction = 'HOLD',
                              offset='HOLD',
                              volume=None
                              )
            break

        if res[res['mark']<0].shape[0] == 0:
            QA_util_log_info('无卖出动作 ==================== {}'.format(trading_date), ui_log=None)
        else:
            for i in res[res['mark'] < 0].index:
                cnt = float(res.at[i, 'cnt'])
                tar = float(res.at[i, '股票余额'])
                NAME = res.at[i, 'NAME']
                INDUSTRY = res.at[i, 'INDUSTRY']
                mark = abs(float(res.at[i, 'mark']))
                price = round(float(res.at[i, 'close']*1.0995),2)

                real = get_StockPos(i, client, account)

                if float(res.at[i, 'cnt']) < real:
                    mark = abs(real - float(res.at[i, 'cnt']))

                if type == 'end':
                    QA_util_log_info('卖出 {code}({NAME},{INDUSTRY}) {cnt}股, 目标持仓:{target},总金额:{tar}'.format(code=i,
                                                                                                NAME= NAME,
                                                                                                INDUSTRY= INDUSTRY,
                                                                                                cnt=abs(mark),
                                                                                                target=cnt,
                                                                                                tar=tar), ui_log=None)
                    e = send_trading_message(account, strategy_id, account_info, i, NAME, INDUSTRY, mark, direction = 'SELL', type='MARKET', priceType=4, price=None, client=client)

                elif type == 'morning':
                    QA_util_log_info('早盘挂单卖出 {code}({NAME},{INDUSTRY}) {cnt}股, 目标持仓:{target},单价:{price},总金额:{tar}'.format(code=i,
                                                                                                NAME= NAME,
                                                                                                INDUSTRY= INDUSTRY,
                                                                                                cnt=abs(mark),
                                                                                                target=cnt,
                                                                                                tar=tar,
                                                                                                price=price), ui_log=None)
                    e = send_trading_message(account, strategy_id, account_info, i, NAME, INDUSTRY, mark, direction = 'SELL', type='LIMIT', priceType=None, price=price, client=client)
                else:
                    pass
                time.sleep(3)

            time.sleep(10)

        if res[res['mark'] == 0].shape[0] == 0:
            QA_util_log_info('无持续持仓动作 ==================== {}'.format(trading_date), ui_log=None)
        else:
            for i in res[res['mark'] == 0].index:
                cnt = float(res.at[i, 'cnt'])
                tar = float(res.at[i, 'real'])
                NAME = res.at[i, 'NAME']
                INDUSTRY = res.at[i, 'INDUSTRY']
                mark = abs(float(res.at[i, 'mark']))
                QA_util_log_info('继续持有 {code}({NAME},{INDUSTRY}), 目标持仓:{target},总金额:{tar}'.format(code=i,
                                                                                       NAME= NAME,
                                                                                       INDUSTRY=INDUSTRY,
                                                                                       target=cnt,
                                                                                       tar=tar), ui_log=None)
                send_actionnotice(strategy_id,
                                  account_info,
                                  '{code}({NAME},{INDUSTRY})'.format(code=i,NAME= NAME, INDUSTRY=INDUSTRY),
                                  direction = 'HOLD',
                                  offset='HOLD',
                                  volume=abs(mark)
                                  )
                time.sleep(3)

        if res[res['mark'] > 0].shape[0] == 0:
            QA_util_log_info('无买入动作 ==================== {}'.format(trading_date), ui_log=None)
        else:
            for i in res[res['mark'] > 0].index:
                cnt = float(res.at[i, 'cnt'])
                tar = float(res.at[i, 'real'])
                NAME = res.at[i, 'NAME']
                INDUSTRY = res.at[i, 'INDUSTRY']
                mark = abs(float(res.at[i, 'mark']))
                price = round(float(res.at[i, 'close']*(1-0.0995)),2)


                if float(res.at[i, '股票余额']) < get_StockPos(i, client, account):
                    pass

                if type == 'end':
                    price = QA_fetch_get_stock_realtm_bid(i)+0.01
                    ####check account usefull capital
                    UseCapital = get_UseCapital(client, account)
                    while float(res.at[i, 'real']) > UseCapital:
                        QA_util_log_info('##JOB {name}({code}){INDUSTRY} 交易资金不足  目标买入{cnt} 预估资金{tar} 实际资金{capital}===={date}'.format(date=trading_date,
                                                                                                         code=i,
                                                                                                         NAME= NAME,
                                                                                                         INDUSTRY=INDUSTRY,
                                                                                                         cnt=abs(mark),
                                                                                                         target=cnt,
                                                                                                         tar=tar,
                                                                                                         capital=UseCapital), ui_log=None)
                        send_actionnotice(strategy_id,
                                          '交易报告:{}'.format(trading_date),
                                          '资金不足',
                                          direction = '缺少资金',
                                          offset='HOLD',
                                          volume=float(float(res.at[i, 'real']) - get_UseCapital(client, account)))
                        time.sleep(5)
                        QA_util_log_info('##JOB01 Now Got Account Info ===={code} {name} {real} {date} {capital}'.format(str(trading_date)), ui_log=None)

                    QA_util_log_info('买入 {code}({NAME},{INDUSTRY}) {cnt}股, 目标持仓:{target},单价:{price},总金额:{tar}'.format(code=i,
                                                                                                NAME= NAME,
                                                                                                INDUSTRY=INDUSTRY,
                                                                                                cnt=abs(mark),
                                                                                                target=cnt,
                                                                                                price=price,
                                                                                                tar=tar), ui_log=None)
                    #e = send_trading_message(account, strategy_id, account_info, i, NAME, INDUSTRY, mark, direction = 'BUY', type='MARKET', priceType=4, price = None, client=client)
                    e = send_trading_message(account, strategy_id, account_info, i, NAME, INDUSTRY, mark, direction = 'BUY', type='LIMIT', priceType=None, price=price, client=client)
                    time.sleep(5)

                elif type == 'morning':
                    QA_util_log_info('早盘挂单买入 {code}({NAME},{INDUSTRY}) {cnt}股, 目标持仓:{target},单价:{price},总金额:{tar}'.format(code=i,
                                                                                                NAME= NAME,
                                                                                                INDUSTRY=INDUSTRY,
                                                                                                cnt=abs(mark),
                                                                                                target=cnt,
                                                                                                price=price,
                                                                                                tar=tar), ui_log=None)
                    e = send_trading_message(account, strategy_id, account_info, i, NAME, INDUSTRY, mark, direction = 'BUY', type='LIMIT', priceType=None, price=price, client=client)

                    time.sleep(5)
                else:
                    QA_util_log_info('type 参数错误 {type} 必须为 [morning, end]'.format(type=type), ui_log=None)

                time.sleep(3)

            time.sleep(15)

        if type == 'end':
            sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)
            sub_accounts = sub_accounts - frozen
            res = build(target, positions, sub_accounts, percent, True, 100)
        elif type == 'morning':
            break
        else:
            break
    QA_util_log_info('交易完成 ==================== {}'.format(trading_date), ui_log=None)
    send_actionnotice(strategy_id,
                      '交易报告:{}'.format(trading_date),
                      '交易完成',
                      direction = 'HOLD',
                      offset='HOLD',
                      volume=None
                      )

    return(res1)

if __name__ == '__main__':
    pass