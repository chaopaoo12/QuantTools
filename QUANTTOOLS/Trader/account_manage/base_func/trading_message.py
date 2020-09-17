from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTAXIS.QAUtil import QA_util_log_info

def Sell(client, account1, symbol, NAME, INDUSTRY, type, priceType, amount, price):
    if type == 'MARKET':
        QA_util_log_info('最优五档卖出 {code}({NAME},{INDUSTRY}) {cnt}股'.format(code=symbol, NAME= NAME,
                                                                          INDUSTRY= INDUSTRY,cnt=amount), ui_log=None)
        client.sell(account1, symbol=symbol, type=type, priceType=priceType, amount=amount)
    elif type == 'LIMIT':
        QA_util_log_info('限价卖出 {code}({NAME},{INDUSTRY}) {cnt}股 单价:{price} 总价:{tar}'.format(code=symbol, NAME= NAME,
                                                                                            INDUSTRY= INDUSTRY,cnt=amount, price=price, tar=price*amount), ui_log=None)
        client.sell(account1, symbol=symbol, type=type, amount=amount , price = price)
    else:
        QA_util_log_info('Direction 参数错误 type{type} 必须为 [SELL, LIMIT]'.format(type=type), ui_log=None)

def Buy(client, account1, symbol, NAME, INDUSTRY, type, priceType, amount, price):
    if type == 'MARKET':
        QA_util_log_info('最优五档买入 {code}({NAME},{INDUSTRY}) {cnt}股'.format(code=symbol, NAME= NAME,
                                                                          INDUSTRY= INDUSTRY,cnt=amount), ui_log=None)
        client.buy(account1, symbol=symbol, type=type, priceType=priceType, amount=amount)
    elif type == 'LIMIT':
        QA_util_log_info('限价买入 {code}({NAME},{INDUSTRY}) {cnt}股 单价:{price} 总价:{tar}'.format(code=symbol, NAME= NAME,
                                                                                            INDUSTRY= INDUSTRY,cnt=amount, price=price, tar=price*amount), ui_log=None)
        client.buy(account1, symbol=symbol, type=type, amount=amount , price = price)
    else:
        QA_util_log_info('Direction 参数错误 {type} 必须为 [SELL, LIMIT]'.format(type=type), ui_log=None)

def send_trading_message(account1, strategy_id, account_info, code, NAME, INDUSTRY, mark, direction, type, priceType, price, client):
    codes = []
    try:
        send_actionnotice(strategy_id,
                          account_info,
                          '{code}({NAME},{INDUSTRY})'.format(code=code,NAME= NAME, INDUSTRY=INDUSTRY),
                          direction = direction,
                          offset='OPEN',
                          volume=abs(mark),
                          price = price
                          )

        if direction == 'SELL':
            Sell(client, account1, code, NAME, INDUSTRY, type, priceType, abs(mark), price)
        elif direction == 'BUY':
            Buy(client, account1, code, NAME, INDUSTRY, type, priceType, abs(mark), price)
        elif direction == 'HOLD':
            QA_util_log_info('保持持股 {code}({NAME},{INDUSTRY}) {cnt}股'.format(code=code, NAME= NAME,
                                                                              INDUSTRY= INDUSTRY,cnt=abs(mark)), ui_log=None)
            pass

    except Exception as e:
        send_actionnotice(strategy_id,
                          account_info,
                          '{code}({NAME},{INDUSTRY}) 交易失败'.format(code=code,NAME= NAME, INDUSTRY=INDUSTRY),
                          direction = direction,
                          offset= 'OPEN',
                          volume=abs(mark)
                          )
        QA_util_log_info('{type}交易失败 {code}({NAME},{INDUSTRY}) {cnt}股'.format(type=type, code=code, NAME= NAME,
                                                                          INDUSTRY= INDUSTRY,cnt=abs(mark)), ui_log=None)
        print(e)
        codes.append(code)
    return(codes)