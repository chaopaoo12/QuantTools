from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Trader.account_manage.base_func.trading_message import send_trading_message
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_StockCapital
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_realtm_ask
import math

def SELL(client, account, strategy_id, account_info, trading_date,
         code, name, industry, target_capital, close=0, type='end', test=False):
    # 买股按金额 给定金额条件下 最大数量
    # 卖股按金额 给定金额条件下 最大数量
    real_capital = get_StockCapital(code, client, account)

    if real_capital > target_capital:
        deal_capital = real_capital - target_capital
    else:
        deal_capital = 0

    QA_util_log_info('##JOB Get Real Time Postition {real_capital} Before {code} Selling {deal_capital} ===== {date}'.format(
        real_capital=real_capital, deal_capital=deal_capital, code=code, date=trading_date), ui_log=None)

    if type == 'end':

        price = QA_fetch_get_stock_realtm_ask(code)
        if price <= 10:
            price = price
        else:
            price = round(price-0.01, 2)
        deal_pos = math.floor(round(deal_capital/price, 0)/100) * 100
        # 如果只有卖出部分不满100 ?? 单价较贵的票容易有这个问题
        # 应全额卖出 做出if判断
        # 可以继续持有 保持现有代码

        QA_util_log_info('##JOB Get Real Time Price {price} 需卖出{deal_pos} Before {code} Selling ===== {date}'.format(
            price=price, code=code, deal_pos=deal_pos, date=trading_date), ui_log = None)

        QA_util_log_info('卖出 {code}({NAME},{INDUSTRY}){deal_pos}股, 目标持仓金额:{target_capital}====={trading_date}'.format(
            code=code, NAME= name, INDUSTRY=industry, deal_pos=abs(deal_pos),
            target_capital=target_capital, price=price, trading_date=trading_date),ui_log=None)
        if test is True:
            e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos,
                                     direction='SELL', type='MARKET', priceType=4, price=None, client=client)
            # e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos, direction = 'SELL', type='LIMIT', priceType=None, price=price, client=client)
        else:
            QA_util_log_info('Test Mode', ui_log=None)

    elif type == 'morning':
        if str(code).startswith('300') is True:
            low_value = 0.1995
        else:
            low_value = 0.0995
        price = round(float(close * (1 + low_value)), 2)
        deal_pos = math.floor(round(deal_capital/price, 0)/100) * 100
        # 如果只有卖出部分不满100 ?? 单价较贵的票容易有这个问题
        # 应全额卖出 做出if判断
        # 可以继续持有 保持现有代码

        QA_util_log_info('##JOB Get Down Price {price} 需卖出{deal_pos} Before {code} Selling ===== {date}'.format(
            price=price, code=code, deal_pos=deal_pos, date=trading_date), ui_log=None)

        QA_util_log_info('早盘挂单卖出 {code}({NAME},{INDUSTRY}){deal_pos}股,目标持仓金额:{target_capital}====={trading_date}'.format(
            code=code, NAME=name, INDUSTRY=industry, deal_pos=abs(deal_pos),
            target_capital=target_capital, price=price,
            trading_date=trading_date), ui_log=None)

        if test is True:
            e = send_trading_message(account, strategy_id, account_info, code, name, industry, deal_pos,
                                     direction='SELL', type='LIMIT', priceType=None, price=price, client=client)
        else:
            QA_util_log_info('Test Mode', ui_log=None)
    else:
        QA_util_log_info('type 参数错误 {type} 必须为 [morning, end]====={trading_date}'.format(
            type=type, trading_date=trading_date), ui_log=None)

if __name__ == 'main':
    pass