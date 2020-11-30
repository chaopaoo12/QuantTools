
from .base_tools import trends_money, trends_btc, trends_gold, trends_stock, check, trends_stock_hour, check_hour

def btc_daily(BTC):
    day, week = trends_btc(BTC)
    day_check = check(day).loc[BTC]
    week_check = check(week).loc[BTC]
    return(day_check, week_check)

def money_daily(MONEY, date):
    day, week = trends_money(MONEY, date)
    day_check = check(day).loc[MONEY]
    week_check = check(week).loc[MONEY]
    return(day_check, week_check)

def gold_daily(GOLD, date):
    day, week = trends_gold(GOLD, date)
    day_check = check(day).loc[GOLD]
    week_check = check(week).loc[GOLD]
    return(day_check, week_check)

def stock_daily(stock, start_date, end_date):
    day, week = trends_stock(stock,start_date,end_date)
    day_check = check(day).loc[stock]
    week_check = check(week).loc[stock]
    return(day_check, week_check)

def stock_hourly(stock, start_date, end_date, date_type):
    hour = trends_stock_hour(stock,start_date,end_date)
    date = end_date + ' ' + date_type
    hour_check = check_hour(hour, date).loc[stock]
    return(hour_check)