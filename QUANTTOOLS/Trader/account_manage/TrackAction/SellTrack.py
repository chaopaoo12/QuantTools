from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
import easyquotation

def SellTrack(strategy_id, trading_date, code, name, industry, close):
    quotation = easyquotation.use('sina')
    ask_price = quotation.real(code)[code]['sell']
    ask_vol = quotation.real(code)[code]['ask1_volume']

    #ask_vol = QA_fetch_get_stock_realtm_askvol(code)
    #ask_price = QA_fetch_get_stock_realtm_ask(code)
    ask_pct = (ask_price - close)/close

    if abs(ask_pct) >= 0.09:
        ask_mark = True
    elif abs(ask_pct) >= 0.05:
        ask_mark = True
    elif ask_vol == 0:
        ask_mark = True
    else:
        ask_mark = False

    if ask_mark:
        QA_util_log_info('##JOB Selling Tracking {name}({code}){industry} 卖价{ask_price} 上涨:{ask_pct}===={date}'.format(date=trading_date,
                                                                                                                      code=code,
                                                                                                                      name= name,
                                                                                                                      industry=industry,
                                                                                                                      ask_price=ask_price,
                                                                                                                      ask_pct=ask_pct), ui_log=None)
        send_actionnotice(strategy_id,
                          '卖出跟踪报告:{}'.format(trading_date),
                          '{name}({code})--{industry}上涨'.format(name=name,code=code,industry=industry),
                          direction = 'SELL',
                          offset=ask_price,
                          volume=ask_pct)

