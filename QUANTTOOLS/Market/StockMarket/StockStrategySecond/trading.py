from QUANTTOOLS.Message import send_email, send_actionnotice
from QUANTTOOLS.Trader import trade_roboot, get_Client,check_Client
from .setting import working_dir, percent, exceptions
from QUANTAXIS.QAUtil import QA_util_log_info
from .concat_predict import concat_predict,load_prediction,check_prediction
import time
import datetime

def trading(trading_date, percent=percent, strategy_id= '机器学习1号', account= 'name:client-1', working_dir= working_dir, exceptions= exceptions, ui_log= None):

    QA_util_log_info('##JOB01 Now Predict ==== {}'.format(str(trading_date)), ui_log)
    try:
        prediction = load_prediction('prediction', working_dir)
        check_prediction(prediction, trading_date)
        tar = prediction['tar']
    except:
        tar,tar_index,index_tar,safe_tar,stock_tar,start,end,model_date = concat_predict(trading_date, strategy_id=strategy_id,  working_dir=working_dir)

    try:
        r_tar = tar.loc[trading_date][['NAME','INDUSTRY','Z_PROB','O_PROB','RANK']]
    except:
        r_tar = None
        send_email('交易报告:'+ trading_date, "空仓状态", 'date')

    QA_util_log_info(
        '##JOB03 Now Chect Account Server ==== {}'.format(str(trading_date)), ui_log)
    client = get_Client()
    check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)
    send_actionnotice(strategy_id,
                      '交易报告:{}'.format(trading_date),
                      '交易准备已完成',
                      direction = 'HOLD',
                      offset='HOLD',
                      volume=None
                      )
    '##JOB04 Now Timing Control ==== {}'
    tm = int(datetime.datetime.now().strftime("%H%M%S"))
    target_tm = int(time.strftime("%H%M%S", time.strptime("14:50:00", "%H:%M:%S")))
    while tm < target_tm:
        tm = int(datetime.datetime.now().strftime("%H%M%S"))
        time.sleep(15)

    QA_util_log_info(
        '##JOB04 Now Trading ==== {}'.format(str(trading_date)), ui_log)
    send_actionnotice(strategy_id,
                      '交易报告:{}'.format(trading_date),
                      '进入交易时段',
                      direction = 'HOLD',
                      offset='HOLD',
                      volume=None
                      )
    res = trade_roboot(r_tar, account, trading_date, percent, strategy_id, type='end', exceptions = exceptions)
    return(res)

if __name__ == '__main__':
    pass
