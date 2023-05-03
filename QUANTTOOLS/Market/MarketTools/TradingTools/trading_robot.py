from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.Message.message_func.wechat import send_actionnotice
from QUANTTOOLS.Trader.account_manage.base_func.Client import get_UseCapital
from QUANTTOOLS.Trader.account_manage.TradAction.BUY import BUY
from QUANTTOOLS.Trader.account_manage.TradAction.SELL import SELL
import time
from QUANTTOOLS.Market.MarketTools.TimeTools.time_control import check_market_time
from gmtrade.api import order_cancel_all

def trading_robot(client, account, account_info, signal_data, trading_date, mark_tm, title, test=False):

    QA_util_log_info('JOB Start Trading Action ==================== {}'.format(
        mark_tm), ui_log=None)

    # action
    # ckeck whether not market time
    while check_market_time(test=test) is False:
        time.sleep(60)
        pass

    # cancel last mark time action
    QA_util_log_info('JOB Cancel Last Mark Time Trading Action ==================== {}'.format(
        mark_tm), ui_log=None)
    if signal_data is not None and test is False:
        if client.type in ['yun_ease','loacl']:
            try:
                client.client.cancel_all(account)
            except:
                QA_util_log_info('JOB Cancel Last Mark Time Trading Action Failed')
        elif client.type == 'sim_myquant':
            order_cancel_all()
        else:
            pass

    if signal_data is not None:
        if signal_data['sell'] is not None:
            for sell_list in signal_data['sell']:
                QA_util_log_info('##JOB Now Start Selling {code} ==== {stm}{msg}'.format(
                    code=sell_list['code'], stm=str(mark_tm), msg=sell_list['msg']), ui_log = None)

                send_actionnotice(title,'{code}{name}:{stm}{msg}'.format(
                    code=sell_list['code'], name=sell_list['name'], stm=mark_tm, msg=sell_list['msg']),
                                  '卖出信号', direction='SELL', offset=mark_tm, volume=None)
                # sell
                if test is False:
                    if client.type in ['yun_ease','loacl']:
                        try:
                            client.client.cancel_all(account)
                        except:
                            QA_util_log_info('JOB Cancel Last Trading Action Failed')
                    elif client.type == 'sim_myquant':
                        order_cancel_all()
                    else:
                        pass

                SELL(client, account, title, account_info, trading_date,
                     sell_list['code'], sell_list['name'], sell_list['industry'],
                     target_capital=sell_list['target_capital'], close=sell_list['close'],
                     type='MARKET', test=test)
        else:
            QA_util_log_info('##JOB No Selling Signal ==== {stm}'.format(
                stm=str(mark_tm)), ui_log = None)

        if signal_data['buy'] is not None:
            for buy_list in signal_data['buy']:
                QA_util_log_info('##JOB Now Start Buying {code} ===== {stm}{msg}'.format(
                    code=buy_list['code'], stm=str(mark_tm), msg=buy_list['msg']), ui_log = None)

                send_actionnotice(title,'{code}{name}:{stm}{msg}'.format(
                    code=buy_list['code'], name=buy_list['name'], stm=mark_tm, msg=buy_list['msg']),
                                  '买入信号', direction='BUY', offset=mark_tm, volume=None)
                if get_UseCapital(client, account) > 3000:
                    # buy
                    BUY(client, account, title, account_info, trading_date,
                        buy_list['code'], buy_list['name'], buy_list['industry'],
                        target_capital=buy_list['target_capital'], close=buy_list['close'],
                        type='LIMIT', test=test)
        else:
            QA_util_log_info('##JOB No Buying Signal ==== {stm}'.format(
                stm=str(mark_tm)), ui_log = None)

        QA_util_log_info('本时段交易完成 ==================== {} {}'.format(
            trading_date, mark_tm), ui_log=None)
    else:
        QA_util_log_info('本时段无交易数据 ==================== {} {}'.format(
            trading_date, mark_tm), ui_log=None)


if __name__ == '__main__':
    pass