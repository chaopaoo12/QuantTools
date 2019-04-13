import datetime
from dateutil.relativedelta import relativedelta

def QA_util_add_days(dt, days):
    """
    #返回dt隔months个月后的日期，months相当于步长
    """
    dt = datetime.datetime.strptime(
        dt, "%Y-%m-%d") + relativedelta(days=days)
    return(dt)

def QA_util_add_years(dt, days):
    """
    #返回dt隔months个月后的日期，months相当于步长
    """
    dt = datetime.datetime.strptime(
        dt, "%Y-%m-%d") + relativedelta(years=days)
    return(dt)

def QA_util_getBetweenYear(from_date, to_date):
    """
    #返回所有月份，以及每月的起始日期、结束日期，字典格式
    """
    date_list = {}
    begin_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(to_date, "%Y-%m-%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y")
        date_list[date_str] = date_str
        begin_date = begin_date + relativedelta(years=1)
    return(date_list)


def QA_util_get_1st_of_next_month(dt):
    """
    获取下个月第一天的日期
    :return: 返回日期
    """
    year = dt.year
    month = dt.month
    if month == 12:
        month = 1
        year += 1
    else:
        month += 1
    res = datetime.datetime(year, month, 1)
    return res