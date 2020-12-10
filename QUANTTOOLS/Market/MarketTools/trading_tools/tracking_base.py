from QUANTTOOLS.Market.MarketTools.trading_tools import load_data
from QUANTTOOLS.Market.MarketTools.trading_tools.track import track_roboot
from QUANTAXIS.QAUtil import QA_util_log_info
import time
import datetime

def tracking_morning(trading_date, func, model_name, file_name, percent, account, working_dir, exceptions):

    start_time = '09:25:00'
    end_time = '11:30:00'
    res = tracking_base(trading_date, func, model_name, file_name, start_time, end_time, percent, account, working_dir, exceptions)
    return(res)

def tracking_afternon(trading_date, func, model_name, file_name, percent, account, working_dir, exceptions):
    start_time = '13:00:00'
    end_time = '15:00:00'
    res = tracking_base(trading_date, func, model_name, file_name, start_time, end_time, percent, account, working_dir, exceptions)
    return(res)

def tracking_base(trading_date, func, model_name, file_name, start_time, end_time, percent, account, working_dir, exceptions):

    '##JOB## Now Timing Control ===== {}'
    tm = int(datetime.datetime.now().strftime("%H%M%S"))
    target_tm = int(time.strftime("%H%M%S", time.strptime(start_time, "%H:%M:%S")))
    while tm < target_tm:
        tm = int(datetime.datetime.now().strftime("%H%M%S"))
        time.sleep(60)

    r_tar, prediction_tar = load_data(func, trading_date, working_dir, model_name, file_name)

    QA_util_log_info('##JOB## Now Tracking ===== {}'.format(str(trading_date)))

    res = track_roboot(r_tar, account, trading_date, percent, model_name, start_time, end_time, exceptions = exceptions)
    return(res)

if __name__ == '__main__':
    pass