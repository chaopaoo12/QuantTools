from QUANTAXIS.QAUtil import QA_util_get_last_day, QA_util_today_str, QA_util_log_info
from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_realtm_ask,QA_fetch_get_stock_realtm_bid,QA_fetch_get_stock_close
from QUANTTOOLS.account_manage.trading_message import send_trading_message
from QUANTTOOLS.account_manage.Client import get_Client,check_Client,get_UseCapital,get_AllCapital,get_StockPos
import pandas as pd
import time
import datetime
import math

def BUY(client, account, strategy_id, account_info,trading_date, code, name, industry, deal_pos, target_pos, target, close, type = 'end'):

    real_pos = get_StockPos(code, client, account)

    if target_pos > real_pos:
        deal_pos = abs(real_pos - target_pos)

    if type == 'end':
        price = QA_fetch_get_stock_realtm_bid(code)+0.01
        ####check account usefull capital
        UseCapital = get_UseCapital(client, account)
        while (price * deal_pos) > UseCapital:
            QA_util_log_info('##JOB {name}({code}){INDUSTRY} 交易资金不足 目标买入{deal_pos}股 预估资金{target} 实际资金{capital}===={date}'.format(date=trading_date,
                                                                                                                                 code=code,
                                                                                                                                 NAME= name,
                                                                                                                                 INDUSTRY=industry,
                                                                                                                                 deal_pos=abs(deal_pos),
                                                                                                                                 target=(price * deal_pos),
                                                                                                                                 capital=UseCapital), ui_log=None)
            send_actionnotice(strategy_id,
                              '交易报告:{}'.format(trading_date),
                              '资金不足',
                              direction = 'BUY',
                              offset='缺少资金',
                              volume=(price * deal_pos) - UseCapital)
            time.sleep(5)

        QA_util_log_info('买入 {code}({NAME},{INDUSTRY}) {deal_pos}股, 目标持仓:{target_pos},单价:{price},总金额:{target}'.format(code=code,
                                                                                                                      NAME= name,
                                                                                                                      INDUSTRY=industry,
                                                                                                                      deal_pos=abs(deal_pos),
                                                                                                                      target_pos=target_pos,
                                                                                                                      price=price,
                                                                                                                      target=abs(deal_pos)*price), ui_log=None)
        #e = send_trading_message(account, strategy_id, account_info, i, NAME, INDUSTRY, deal, direction = 'BUY', type='MARKET', priceType=4, price = None, client=client)
        e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos, direction = 'BUY', type='LIMIT', priceType=None, price=price, client=client)
        time.sleep(5)

    elif type == 'morning':
        price = round(float(close*(1-0.0995)),2)
        QA_util_log_info('早盘挂单买入 {code}({NAME},{INDUSTRY}) {deal_pos}股, 目标持仓:{target_pos},单价:{price},总金额:{target}'.format(code=code,
                                                                                                                          NAME= name,
                                                                                                                          INDUSTRY=industry,
                                                                                                                          cnt=abs(deal_pos),
                                                                                                                          target_pos=target_pos,
                                                                                                                          price=price,
                                                                                                                          target=target), ui_log=None)
        e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos, direction = 'BUY', type='LIMIT', priceType=None, price=price, client=client)

        time.sleep(5)
    else:
        QA_util_log_info('type 参数错误 {type} 必须为 [morning, end]'.format(type=type), ui_log=None)

