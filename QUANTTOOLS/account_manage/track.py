from QUANTAXIS.QAUtil import  QA_util_log_info
from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTTOOLS.account_manage.trading_message import send_trading_message
from QUANTTOOLS.account_manage.Client import get_Client,check_Client,get_UseCapital,get_AllCapital,get_StockPos
from QUANTTOOLS.account_manage.BuyTrack import BuyTrack
from QUANTTOOLS.account_manage.SellTrack import SellTrack
from QUANTTOOLS.account_manage.BuildTradingFrame import build
import time
import datetime

def track_roboot(target_tar, account, trading_date, percent, strategy_id, exceptions = None):

    QA_util_log_info('##JOB Now Get Account info ==== {}'.format(str(trading_date)), ui_log = None)
    client = get_Client()
    sub_accounts, frozen, positions, frozen_positions = check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)

    QA_util_log_info('##JOB Now Build Tracking Frame ==== {}'.format(str(trading_date)), ui_log = None)
    res = build(target_tar, positions, sub_accounts, percent, False)

    if res is not None:

        QA_util_log_info('##JOB Now Check Timing ==== {}'.format(str(trading_date)), ui_log = None)

        target_ea = int(time.strftime("%H%M%S", time.strptime("09:25:00", "%H:%M:%S")))
        target_bk1 = int(time.strftime("%H%M%S", time.strptime("11:30:00", "%H:%M:%S")))
        target_bk2 = int(time.strftime("%H%M%S", time.strptime("13:00:00", "%H:%M:%S")))
        target_af = int(time.strftime("%H%M%S", time.strptime("15:00:00", "%H:%M:%S")))
        tm = int(datetime.datetime.now().strftime("%H%M%S"))
        if tm < target_ea or tm >= target_af:
            QA_util_log_info('已过交易时段 {hour} ==================== {data}'.format(hour = tm, date = trading_date), ui_log=None)
            send_actionnotice(strategy_id,'交易报告:{}'.format(trading_date),'已过交易时段',direction = 'HOLD',offset='HOLD',volume=None)

        QA_util_log_info('##JOB Now Start Tracking ==== {}'.format(str(trading_date)), ui_log = None)
        while tm >= target_ea  or tm <= target_af:

            if tm > target_bk1 and tm < target_bk2:
                time.sleep(120)
                tm = int(datetime.datetime.now().strftime("%H%M%S"))
                continue

            QA_util_log_info('##JOB Now Tracking Selling ==== {}'.format(str(trading_date)), ui_log = None)
            if res[res['deal']<0].shape[0] == 0:
                QA_util_log_info('##JOB None Selling Tracking ==================== {}'.format(trading_date), ui_log=None)
            else:
                for code in res[res['deal'] < 0].index:
                    name = res.loc[code]['NAME']
                    industry = res.loc[code]['INDUSTRY']
                    close = float(res.loc[code]['close'])

                    QA_util_log_info('##JOB Now Start Tracking Selling {code} ==== {date}'.format(code = code, date = str(trading_date)), ui_log = None)
                    SellTrack(strategy_id, trading_date, code, name, industry, close)

            QA_util_log_info('##JOB Now Tracking Buying ==== {}'.format(str(trading_date)), ui_log = None)
            if res[res['deal'] > 0].shape[0] == 0:
                QA_util_log_info('##JOB None Buying Tracking ==================== {}'.format(trading_date), ui_log=None)
            else:
                for code in res[res['deal'] > 0].index:
                    name = res.loc[code]['NAME']
                    industry = res.loc[code]['INDUSTRY']
                    close = float(res.loc[code]['close'])

                    QA_util_log_info('##JOB Now Start Tracking Buying {code} ==== {date}'.format(code = code, date = str(trading_date)), ui_log = None)
                    BuyTrack(strategy_id, trading_date, code, name, industry, close)

            time.sleep(300)
            tm = int(datetime.datetime.now().strftime("%H%M%S"))

        QA_util_log_info('##JOB Tracking Finished ==================== {}'.format(trading_date), ui_log=None)
        send_actionnotice(strategy_id,'Tracking Report:{}'.format(trading_date),'Tracking Finished',direction = 'Tracking',offset='Finished',volume=None)
    else:
        QA_util_log_info('JOB None Tracking Target ==================== {}'.format(trading_date), ui_log=None)
        send_actionnotice(strategy_id,'None Tracking Report:{}'.format(trading_date),'Tracking Finished',direction = 'Tracking',offset='Finished',volume=None)

if __name__ == '__main__':
    pass