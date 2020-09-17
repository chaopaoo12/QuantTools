from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Trader.account_manage.base_func.trading_message import send_trading_message
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_StockPos
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_realtm_ask
import easyquotation
import math

def SELL(client, account, strategy_id, account_info, trading_date, code, name, industry, deal_pos, target_pos, target, close, type = 'end', test = False):

    real_pos = get_StockPos(code, client, account)
    QA_util_log_info('##JOB Get Real Time Postition {real_pos} Before {code} Buying ===== {date}'.format(real_pos=real_pos, code = code, date=trading_date), ui_log = None)

    if math.floor(target_pos/100)*100 < target_pos:
        pass
    else:
        deal_pos = math.floor(abs(real_pos - target_pos)/100)*100

    QA_util_log_info('##JOB Get Refresh Deal Position {deal_pos} Before {code} Buying ===== {date}'.format(deal_pos = deal_pos, code = code, date=trading_date), ui_log = None)

    if type == 'end':
        #quotation = easyquotation.use('sina')
        #price = quotation.real(code)[code]['sell']-0.01
        price = QA_fetch_get_stock_realtm_ask(code)-0.01
        QA_util_log_info('##JOB Get Real Time Price {price} Before {code} Buying ===== {date}'.format(price = price, code = code, date=trading_date), ui_log = None)

        QA_util_log_info('卖出 {code}({NAME},{INDUSTRY}) {deal_pos}股, 目标持仓:{target_pos},单价:{price},总金额:{target}====={trading_date}'.format(code=code,
                                                                                                                              NAME= name,
                                                                                                                              INDUSTRY= industry,
                                                                                                                              deal_pos=abs(deal_pos),
                                                                                                                              target_pos=target_pos,
                                                                                                                              target=abs(deal_pos)*price,
                                                                                                                              price=price,
                                                                                                                              trading_date=trading_date),
                         ui_log=None)
        if test == True:
            #e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos, direction = 'SELL', type='MARKET', priceType=4, price=None, client=client)
            e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos, direction = 'SELL', type='LIMIT', priceType=None, price=price, client=client)
        else:
            pass

    elif type == 'morning':
        if str(code).startswith('300') is True:
            low_value = 0.1995
        else:
            low_value = 0.0995
        price = round(float(close * (1 + low_value)),2)

        QA_util_log_info('##JOB Get Down Price {price} Before {code} Selling ===== {date}'.format(price = price, code = code, date=trading_date), ui_log = None)

        QA_util_log_info('早盘挂单卖出 {code}({NAME},{INDUSTRY}) {deal_pos}股, 目标持仓:{target_pos},单价:{price},总金额:{target}====={trading_date}'.format(code=code,
                                                                                                                                             NAME= name,
                                                                                                                                             INDUSTRY= industry,
                                                                                                                                             deal_pos=abs(deal_pos),
                                                                                                                                             target_pos=target_pos,
                                                                                                                                             target=abs(deal_pos)*price,
                                                                                                                                             price=price,
                                                                                                                                             trading_date=trading_date),
                         ui_log=None)
        if test == True:
            e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos, direction = 'SELL', type='LIMIT', priceType=None, price=price, client=client)
        else:
            pass
    else:
        QA_util_log_info('type 参数错误 {type} 必须为 [morning, end]====={trading_date}'.format(type=type,trading_date=trading_date), ui_log=None)

if __name__ == 'main':
    pass