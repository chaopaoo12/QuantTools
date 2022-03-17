from QUANTAXIS.QAUtil import QA_util_log_info
import pandas as pd
import time
import datetime


def on_bar(start, end, sep, breaks):
    date = '2022-02-09'
    tm_rng = pd.date_range(date + ' '+start, date + ' '+end, freq=str(sep)+'min')
    break_tmrng = []
    for break_rng in breaks:
        break_tmp = pd.date_range(date + ' '+break_rng[0], date + ' '+break_rng[1], freq=str(sep)+'min')
        break_tmrng.extend(break_tmp[1:-1])
    res_rng = [str(i)[11:20] for i in tm_rng if i not in break_tmrng]
    res_rng = [i for i in res_rng if i not in [start, end]]
    return(res_rng)

def time_check_before(tm_mark, test=False, ckeck_tm=None):
    if test is False:
        if ckeck_tm is None:
            tm = int(datetime.datetime.now().strftime("%H%M%S"))
            return tm <= int(time.strftime("%H%M%S",time.strptime(tm_mark, "%H:%M:%S")))
        else:
            return ckeck_tm <= int(time.strftime("%H%M%S",time.strptime(tm_mark, "%H:%M:%S")))
    else:
        return True


def time_check_after(tm_mark, test=False, ckeck_tm=None):
    if test is False:
        if ckeck_tm is None:
            tm = int(datetime.datetime.now().strftime("%H%M%S"))
            return tm >= int(time.strftime("%H%M%S",time.strptime(tm_mark, "%H:%M:%S")))
        else:
            return ckeck_tm >= int(time.strftime("%H%M%S",time.strptime(tm_mark, "%H:%M:%S")))
    else:
        return True


def open_check(trading_date, test=False, sleep=60):
    if test is False:
        while time_check_before("09:30:00"):
            time.sleep(sleep)
    else:
        pass
    QA_util_log_info('##Check Trading Time Start ==== {}'.format(str(trading_date)), ui_log = None)


def close_check(trading_date, test=False, sleep=60):
    if test is False:
        while time_check_before("15:00:00"):
            time.sleep(sleep)
    else:
        pass
    QA_util_log_info('##Check Trading Time Finished ==================== {}'.format(trading_date), ui_log=None)


def suspend_check(trading_date, test=False, sleep=60):
    if test is False:
        while time_check_after("11:30:00") and time_check_before("13:00:00"):
            QA_util_log_info('##Check Suspend Start ==================== {}'.format(trading_date), ui_log=None)
            time.sleep(sleep)
    else:
        pass
    QA_util_log_info('##Check Suspend Finished ==================== {}'.format(trading_date), ui_log=None)


def check_market_time(test=False):
    if test is False:
        return((time_check_after("09:30:00") and time_check_before("11:30:00")) or
               (time_check_before("15:00:00") and time_check_after("13:00:00")))
    else:
        return True


def get_on_time(tm, mark_list):
    a = mark_list + [tm]
    a.sort()

    if a.index(tm) <= 1 and a.count(tm) < 2:
        mark_tm = '15:00:00'
    elif tm in mark_list:
        mark_tm = tm
    else:
        mark_tm = a[a.index(tm)-1]

    return(mark_tm)