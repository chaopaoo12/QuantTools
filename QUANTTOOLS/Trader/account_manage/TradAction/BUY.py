from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.QAStockETL.QAFetch.QATdx import QA_fetch_get_stock_realtm_bid
from QUANTTOOLS.Trader.account_manage.base_func.trading_message import send_trading_message
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_UseCapital, get_StockPos
import time
import math

def BUY(client, account, strategy_id, account_info,trading_date, code, name, industry, deal_pos, target_pos, target, close, type = 'end'):

    real_pos = get_StockPos(code, client, account)
    QA_util_log_info('##JOB Get Real Time Postition {real_pos} Before {code} Buying ===== {date}'.format(real_pos=real_pos, code = code, date=trading_date), ui_log = None)

    if target_pos > real_pos:
        deal_pos = math.floor(abs(real_pos - target_pos)/100)*100
    QA_util_log_info('##JOB Get Refresh Deal Position {deal_pos} Before {code} Buying ===== {date}'.format(deal_pos = deal_pos, code = code, date=trading_date), ui_log = None)

    if type == 'end':
        #quotation = easyquotation.use('sina')
        #price = quotation.real(code)[code]['buy']+0.01
        price = QA_fetch_get_stock_realtm_bid(code)+0.01
        QA_util_log_info('##JOB Get Real Time Price {price} Before {code} Buying ===== {date}'.format(price = price, code = code, date=trading_date), ui_log = None)

        ####check account usefull capital
        UseCapital = get_UseCapital(client, account)
        QA_util_log_info('##JOB Check Account Usefull Capital {UseCapital} Before {code} Buying ===== {date}'.format(UseCapital = UseCapital,code = code, date=trading_date), ui_log = None)
        if (price * deal_pos) > UseCapital:
            QA_util_log_info('##JOB {name}({code}){industry} 交易资金不足 目标买入{deal_pos}股 预估资金{target} 实际资金{capital}===={date}'.format(date=trading_date,
                                                                                                                                 code=code,
                                                                                                                                 name= name,
                                                                                                                                 industry=industry,
                                                                                                                                 deal_pos=abs(deal_pos),
                                                                                                                                 target=(price * deal_pos),
                                                                                                                                 capital=UseCapital), ui_log=None)
            send_actionnotice(strategy_id,
                              '交易报告:{}'.format(trading_date),
                              '资金不足',
                              direction = 'BUY',
                              offset='缺少资金',
                              volume=(price * deal_pos) - UseCapital)

            deal_pos = math.floor((UseCapital / price)/100) * 100

        QA_util_log_info('买入 {code}({name},{industry}) {deal_pos}股, 目标持仓:{target_pos},单价:{price},总金额:{target}'.format(code=code,
                                                                                                                      name= name,
                                                                                                                      industry=industry,
                                                                                                                      deal_pos=abs(deal_pos),
                                                                                                                      target_pos=target_pos,
                                                                                                                      price=price,
                                                                                                                      target=abs(deal_pos)*price), ui_log=None)
        #e = send_trading_message(account, strategy_id, account_info, i, NAME, INDUSTRY, deal, direction = 'BUY', type='MARKET', priceType=4, price = None, client=client)
        e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos, direction = 'BUY', type='LIMIT', priceType=None, price=price, client=client)
        time.sleep(5)

    elif type == 'morning':
        if str(code).startswith('300') is True:
            low_value = 0.1995
        else:
            low_value = 0.0995
        price = round(float(close*(1-low_value)),2)

        QA_util_log_info('##JOB Get Down Price {price} Before {code} Selling ===== {date}'.format(price = price, code = code, date=trading_date), ui_log = None)

        QA_util_log_info('早盘挂单买入 {code}({name},{industry}) {deal_pos}股, 目标持仓:{target_pos},单价:{price},总金额:{target}'.format(code=code,
                                                                                                                          name= name,
                                                                                                                          industry=industry,
                                                                                                                          deal_pos=abs(deal_pos),
                                                                                                                          target_pos=target_pos,
                                                                                                                          price=price,
                                                                                                                          target=abs(deal_pos)*price), ui_log=None)
        e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos, direction = 'BUY', type='LIMIT', priceType=None, price=price, client=client)

        time.sleep(5)
    else:
        QA_util_log_info('type 参数错误 {type} 必须为 [morning, end]'.format(type=type), ui_log=None)

if __name__ == 'main':
    pass

