from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.account_manage.trading_message import send_trading_message
from QUANTTOOLS.account_manage.Client import get_StockPos
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_realtm_ask

def SELL(client, account, strategy_id, account_info, trading_date, code, name, industry, deal_pos, target_pos, target, close, type = 'end'):
    QA_util_log_info('##JOB Get Real Time Postition Before {code} Selling ===== {date}'.format(code = code, date=trading_date), ui_log = None)
    real_pos = get_StockPos(code, client, account)

    QA_util_log_info('##JOB Get Refresh Deal Position Before {code} Selling ===== {date}'.format(code = code, date=trading_date), ui_log = None)
    if target_pos < real_pos:
        deal_pos = abs(real_pos - target_pos)

    if type == 'end':
        QA_util_log_info('##JOB Get Real Time Price Before {code} Selling ===== {date}'.format(code = code, date=trading_date), ui_log = None)
        price = QA_fetch_get_stock_realtm_ask(code)-0.01
        QA_util_log_info('卖出 {code}({NAME},{INDUSTRY}) {deal_pos}股, 目标持仓:{target_pos},单价:{price},总金额:{target}====={trading_date}'.format(code=code,
                                                                                                                              NAME= name,
                                                                                                                              INDUSTRY= industry,
                                                                                                                              deal_pos=abs(deal_pos),
                                                                                                                              target_pos=target_pos,
                                                                                                                              target=target,
                                                                                                                              price=price,
                                                                                                                              trading_date=trading_date),
                         ui_log=None)
        e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos, direction = 'SELL', type='MARKET', priceType=4, price=None, client=client)
        #e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos, direction = 'SELL', type='LIMIT', priceType=None, price=price, client=client)

    elif type == 'morning':
        QA_util_log_info('##JOB Get Up Price Before {code} Selling ===== {date}'.format(code = code, date=trading_date), ui_log = None)
        price = round(float(close * 1.0995),2)
        QA_util_log_info('早盘挂单卖出 {code}({NAME},{INDUSTRY}) {deal_pos}股, 目标持仓:{target_pos},单价:{price},总金额:{target}====={trading_date}'.format(code=code,
                                                                                                                                             NAME= name,
                                                                                                                                             INDUSTRY= industry,
                                                                                                                                             deal_pos=abs(deal_pos),
                                                                                                                                             target_pos=target_pos,
                                                                                                                                             target=target,
                                                                                                                                             price=price,
                                                                                                                                             trading_date=trading_date),
                         ui_log=None)
        e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos, direction = 'SELL', type='LIMIT', priceType=None, price=price, client=client)
    else:
        QA_util_log_info('type 参数错误 {type} 必须为 [morning, end]====={trading_date}'.format(type=type,trading_date=trading_date), ui_log=None)