from QUANTAXIS.QAUtil import QA_util_log_info
import easyquotation
from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_get_stock_realtm_ask,QA_fetch_get_stock_realtm_bid,QA_fetch_get_stock_realtm_askvol,QA_fetch_get_stock_realtm_bidvol


def BuyTrack(strategy_id, trading_date, code, name, industry, close):

    quotation = easyquotation.use('sina')
    bid_price = quotation.real(code)[code]['buy']
    bid_vol = quotation.real(code)[code]['bid1_volume']
    #bid_vol = QA_fetch_get_stock_realtm_bidvol(code)
    #bid_price = QA_fetch_get_stock_realtm_bid(code)
    bid_pct = (bid_price - close)/close

    if bid_pct <= -0.05:
        bid_mark = True
    elif bid_pct <= -0.09:
        bid_mark = True
    elif bid_vol == 0:
        bid_mark = True
    else:
        bid_mark = False

    if bid_mark:
        QA_util_log_info('##JOB Buying Tracking {name}({code}){industry} 卖价{bid_price} 下跌:{bid_pct}===={date}'.format(date=trading_date,
                                                                                                                      code=code,
                                                                                                                      name= name,
                                                                                                                      industry=industry,
                                                                                                                      bid_price=bid_price,
                                                                                                                      bid_pct=bid_pct), ui_log=None)
        send_actionnotice(strategy_id,
                          '买入跟踪报告:{}'.format(trading_date),
                          '{name}({code})--{industry}下跌'.format(name=name,code=code,industry=industry),
                          direction = 'BUY',
                          offset=bid_price,
                          volume=bid_pct)

