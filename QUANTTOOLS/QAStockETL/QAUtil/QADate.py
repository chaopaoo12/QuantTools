import datetime
import calendar
from dateutil.relativedelta import relativedelta

def QA_util_add_days(dt, days):
    """
    #返回dt隔months个月后的日期，months相当于步长
    """
    dt = datetime.datetime.strptime(
        dt, "%Y-%m-%d") + relativedelta(days=days)
    return(dt)
