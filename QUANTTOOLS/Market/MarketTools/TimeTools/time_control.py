from QUANTAXIS.QAUtil import QA_util_log_info
import time
import datetime


def time_check_before(tm_mark, ckeck_tm=None):
    if ckeck_tm is None:
        tm = int(datetime.datetime.now().strftime("%H%M%S"))
        return tm <= int(time.strftime("%H%M%S",time.strptime(tm_mark, "%H:%M:%S")))
    else:
        return ckeck_tm <= int(time.strftime("%H%M%S",time.strptime(tm_mark, "%H:%M:%S")))


def time_check_after(tm_mark, ckeck_tm=None):
    if ckeck_tm is None:
        tm = int(datetime.datetime.now().strftime("%H%M%S"))
        return tm >= int(time.strftime("%H%M%S",time.strptime(tm_mark, "%H:%M:%S")))
    else:
        return ckeck_tm >= int(time.strftime("%H%M%S",time.strptime(tm_mark, "%H:%M:%S")))


def open_check(trading_date, sleep=60):
    while time_check_before("09:30:00"):
        time.sleep(sleep)
    QA_util_log_info('##Check Trading Time Start ==== {}'.format(str(trading_date)), ui_log = None)


def close_check(trading_date, sleep=60):
    while time_check_before("15:00:00"):
        time.sleep(sleep)
    QA_util_log_info('##Check Trading Time Finished ==================== {}'.format(trading_date), ui_log=None)

def suspend_check(trading_date, sleep=60):
    while time_check_after("11:30:00") and time_check_before("13:00:00"):
        QA_util_log_info('##Check Suspend Start ==================== {}'.format(trading_date), ui_log=None)
        time.sleep(sleep)
    QA_util_log_info('##Check Suspend Finished ==================== {}'.format(trading_date), ui_log=None)


def check_market_time():
    return((time_check_after("09:30:00") and time_check_before("11:30:00")) or
           (time_check_before("15:00:00") and time_check_after("13:00:00")))


def get_on_time(tm, mark_list):
    a = mark_list + [tm]
    a.sort()

    if a.index(tm) == 0:
        mark_tm = '09:30:00'
    elif a.index(tm) == len(a)-1:
        mark_tm = '15:00:00'
    else:
        mark_tm = a[a.index(tm)-1]

    return(mark_tm)