from QUANTAXIS.QAUtil import QA_util_log_info
import time
import datetime


def time_check_before(tm_mark):
    tm = int(datetime.datetime.now().strftime("%H%M%S"))
    return tm <= int(time.strftime("%H%M%S",time.strptime(tm_mark, "%H:%M:%S")))


def time_check_after(tm_mark):
    tm = int(datetime.datetime.now().strftime("%H%M%S"))
    return tm >= int(time.strftime("%H%M%S",time.strptime(tm_mark, "%H:%M:%S")))


def open_check(trading_date):
    while time_check_before("09:30:00"):
        pass
    QA_util_log_info('##JOB Now Start Trading ==== {}'.format(str(trading_date)), ui_log = None)


def close_check(trading_date):
    while time_check_after("15:00:00"):
        pass
    QA_util_log_info('##JOB Trading Finished ==================== {}'.format(trading_date), ui_log=None)


def suspend_check(trading_date):
    while time_check_after("11:30:00") and time_check_before("13:00:00"):
        pass
    QA_util_log_info('##JOB Trading Finished ==================== {}'.format(trading_date), ui_log=None)


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