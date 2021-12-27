from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.QAStockETL.QAFetch.QATdx import QA_fetch_get_stock_realtm_bid
from QUANTTOOLS.Trader.account_manage.base_func.trading_message import send_trading_message
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_UseCapital, get_StockCapital
import time
import math

def BUY(client, account, strategy_id, account_info, trading_date,
        code, name, industry, target_capital, close=0, type='end', test=False):

    # check account usefull capital
    UseCapital = get_UseCapital(client, account)
    QA_util_log_info('##JOB Check Account Usefull Capital {UseCapital} Before {code} Buying ===== {date}'.format(
        UseCapital=UseCapital, code=code, date=trading_date), ui_log = None)

    real_capital = get_StockCapital(code, client, account)

    if target_capital <= UseCapital:
        deal_capital = target_capital - real_capital
    else:
        deal_capital = UseCapital - real_capital

    QA_util_log_info('##JOB Get Real Time Postition {real_capital} Before {code} Buying {deal_capital} ===== {date}'.format(
            real_capital=real_capital, deal_capital=deal_capital, code=code, date=trading_date), ui_log=None)

    if type == 'end':

        price = QA_fetch_get_stock_realtm_bid(code)
        if price <= 10:
            price = price
        else:
            price = round(price-0.01, 2)
        deal_pos = math.floor(round(deal_capital/price, 0)/100) * 100
        QA_util_log_info('##JOB Get Real Time Price {price} 可买入{deal_pos} Before {code} Buying ===== {date}'.format(
            price=price, code=code, deal_pos=deal_pos, date=trading_date), ui_log=None)

        if (price * deal_pos) > UseCapital:
            QA_util_log_info('##JOB 资金不足 减少一手 {name}({code}){industry} 目标买入{deal_pos}股 预估{target}实际{capital}===={date}'.format(
                date=trading_date, code=code, name=name, industry=industry, deal_pos=abs(deal_pos) - 100,
                target=(price * deal_pos), capital=UseCapital), ui_log=None)
            send_actionnotice(strategy_id,
                              '交易报告:{}'.format(trading_date),
                              '资金不足',
                              direction='BUY',
                              offset='缺少资金 减少一手',
                              volume=(price * deal_pos) - UseCapital)

        QA_util_log_info('买入 {code}({name},{industry}) {deal_pos}股, 目标持仓金额:{target_capital}'.format(
            code=code, name=name, industry=industry, deal_pos=abs(deal_pos),
            price=price, target_capital=target_capital), ui_log=None)
        if test:
            QA_util_log_info('Test Mode', ui_log=None)
        else:
            #e = send_trading_message(account, strategy_id, account_info, i, NAME, INDUSTRY, deal, direction = 'BUY', type='MARKET', priceType=4, price = None, client=client)
            e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos,
                                     direction='BUY', type='LIMIT', priceType=None, price=price, client=client)
        time.sleep(5)

    elif type == 'morning':
        if str(code).startswith('300') is True:
            low_value = 0.1995
        else:
            low_value = 0.0995
        price = round(float(close*(1-low_value)), 2)
        deal_pos = math.floor(round(deal_capital/price, 0)/100) * 100

        QA_util_log_info('早盘挂单买入 {code}({name},{industry}){deal_pos}股,目标持仓金额:{target_capital}'.format(
            code=code, name=name, industry=industry, deal_pos=abs(deal_pos),
            price=price, target_capital=target_capital), ui_log=None)
        if test:
            QA_util_log_info('Test Mode', ui_log=None)
        else:
            e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos,
                                     direction='BUY', type='LIMIT', priceType=None, price=price, client=client)

        time.sleep(5)
    else:
        QA_util_log_info('type 参数错误 {type} 必须为 [morning, end]'.format(type=type), ui_log=None)

if __name__ == 'main':
    pass

