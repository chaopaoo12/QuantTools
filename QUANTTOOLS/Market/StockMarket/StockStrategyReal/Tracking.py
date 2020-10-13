
from .setting import working_dir, percent, exceptions
from .concat_predict import concat_predict,load_prediction,check_prediction

from QUANTTOOLS.Message import send_actionnotice,send_email
from QUANTTOOLS.Trader import track_roboot,get_Client,check_Client,track_morning, track_afternoon

from QUANTAXIS.QAUtil import QA_util_log_info

import time
import datetime

def load_target(trading_date, name, strategy_id= '机器学习1号', account= 'name:client-1', working_dir= working_dir, exceptions= exceptions, ui_log= None):
    QA_util_log_info('##JOB01 Now Predict ===== {}'.format(str(trading_date)), ui_log)
    try:
        prediction = load_prediction(name, working_dir)
        check_prediction(prediction, trading_date)
        tar = prediction['tar']
    except:
        tar,stock_tar,start,end,model_date = concat_predict(trading_date, strategy_id=strategy_id,  working_dir=working_dir)

    try:
        r_tar = tar.loc[trading_date][['NAME','INDUSTRY','Z_PROB','O_PROB','RANK']]
    except:
        r_tar = None
        send_email('交易报告:'+ trading_date, "空仓状态", 'date')

    QA_util_log_info(
        '##JOB03 Now Chect Account Server ===== {}'.format(str(trading_date)), ui_log)
    client = get_Client()
    check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)

    send_actionnotice(strategy_id,
                      '交易报告:{}'.format(trading_date),
                      '交易准备已完成',
                      direction = 'HOLD',
                      offset='HOLD',
                      volume=None
                      )
    return(r_tar)

def Tracking(trading_date, percent=percent, strategy_id= '机器学习1号', account= 'name:client-1', working_dir= working_dir, exceptions= exceptions, ui_log= None):

    r_tar = load_target(trading_date, 'prediction', strategy_id, account, working_dir, exceptions, ui_log)

    '##JOB04 Now Timing Control ===== {}'
    tm = int(datetime.datetime.now().strftime("%H%M%S"))
    target_tm = int(time.strftime("%H%M%S", time.strptime("09:25:00", "%H:%M:%S")))
    while tm < target_tm:
        tm = int(datetime.datetime.now().strftime("%H%M%S"))
        time.sleep(60)

    QA_util_log_info(
        '##JOB04 Now Tracking ===== {}'.format(str(trading_date)), ui_log)
    res = track_morning(r_tar, account, trading_date, percent, strategy_id, exceptions = exceptions)

    r_tar = load_target(trading_date, 'prediction_real', strategy_id, account, working_dir, exceptions, ui_log)

    tm = int(datetime.datetime.now().strftime("%H%M%S"))
    target_tm = int(time.strftime("%H%M%S", time.strptime("13:00:00", "%H:%M:%S")))
    while tm < target_tm:
        tm = int(datetime.datetime.now().strftime("%H%M%S"))
        time.sleep(60)

    QA_util_log_info(
        '##JOB04 Now Tracking ===== {}'.format(str(trading_date)), ui_log)
    res = track_afternoon(r_tar, account, trading_date, percent, strategy_id, exceptions = exceptions)
    return(res)

if __name__ == '__main__':
    pass