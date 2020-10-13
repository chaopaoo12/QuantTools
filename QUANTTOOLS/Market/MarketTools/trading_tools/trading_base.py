from QUANTTOOLS.Market.MarketTools.trading_tools import load_data
from QUANTTOOLS.Message import send_actionnotice
from QUANTTOOLS.Trader import trade_roboot,get_Client,check_Client
from QUANTAXIS.QAUtil import QA_util_log_info
import time
import datetime

def trading_base(trading_date, func, model_name, file_name, percent, account, working_dir, exceptions, test = False):

    r_tar, prediction_tar = load_data(func, trading_date, working_dir, model_name, file_name)

    QA_util_log_info(
        '##JOB## Now Chect Account Server ==== {}'.format(str(trading_date)))
    client = get_Client()
    check_Client(client, account, model_name, trading_date, exceptions=exceptions)
    send_actionnotice(model_name,
                      '交易报告:{}'.format(trading_date),
                      '交易准备已完成',
                      direction = 'HOLD',
                      offset='HOLD',
                      volume=None
                      )

    QA_util_log_info('##JOB## Now Timing Control ==== {}'.format(str(trading_date)))
    tm = int(datetime.datetime.now().strftime("%H%M%S"))
    target_tm = int(time.strftime("%H%M%S", time.strptime("14:50:00", "%H:%M:%S")))
    if test is False:
        while tm < target_tm:
            tm = int(datetime.datetime.now().strftime("%H%M%S"))
            time.sleep(15)

    QA_util_log_info(
        '##JOB## Now Trading ==== {}'.format(str(trading_date)))
    send_actionnotice(model_name,
                      '交易报告:{}'.format(trading_date),
                      '进入交易时段',
                      direction = 'HOLD',
                      offset='HOLD',
                      volume=None
                      )

    res = trade_roboot(r_tar, account, trading_date, percent, model_name, type='end', exceptions = exceptions, test = test)
    return(res)