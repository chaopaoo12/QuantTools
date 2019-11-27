from QUANTTOOLS.message_func.wechat import send_actionnotice

def send_trading_message(account1, strategy_id, account_info, code, NAME, INDUSTRY, mark, direction, type, priceType, client):
    e = []
    try:
        if direction == 'SELL':
            client.sell(account1, symbol=code, type=type, priceType=priceType, amount=abs(mark))
        elif direction == 'BUY':
            client.buy(account1, symbol=code, type=type, priceType=priceType, amount=abs(mark))
        elif direction == 'HOLD':
            pass
        send_actionnotice(strategy_id,
                          account_info,
                          '{code}({NAME},{INDUSTRY})'.format(code=code,NAME= NAME, INDUSTRY=INDUSTRY),
                          direction = direction,
                          offset='OPEN',
                          volume=abs(mark)
                          )
    except:
        send_actionnotice(strategy_id,
                          account_info,
                          '{code}({NAME},{INDUSTRY}) 交易失败'.format(code=code,NAME= NAME, INDUSTRY=INDUSTRY),
                          direction = direction,
                          offset= 'OPEN',
                          volume=abs(mark)
                          )
        e.append(code)
    return(e)