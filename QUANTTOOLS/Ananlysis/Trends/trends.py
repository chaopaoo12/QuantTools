
from .base_tools import trends_money, trends_btc, trends_gold, trends_stock, check, trends_stock_hour, check_hour, trends_btc_hour, trends_future, trends_globalindex

def btc_daily(BTC):
    day, week = trends_btc(BTC)
    day_check = check(day).loc[BTC]
    week_check = check(week).loc[BTC]
    skdj_k = day.iloc[-1:]['SKDJ_K'].values[0]
    skdj_k_wk = week.iloc[-1:]['SKDJ_K'].values[0]
    incr5 = day.iloc[-1:]['MA5'].values[0]
    incr15 = day.iloc[-1:]['MA15'].values[0]
    incr = day.iloc[-1:]['MA15_C'].values[0]
    incrs = day.iloc[-1:]['MA15_D'].values[0]
    mean = day.iloc[-1:]['mean'].values[0]
    per25 = day.iloc[-1:]['per25'].values[0]
    per75 = day.iloc[-1:]['per75'].values[0]
    perc = day.iloc[-1:]['perc'].values[0]
    SKDJ_MARK = day.iloc[-1:]['SKDJ_CROSS2'].values[0] + day.iloc[-1:]['SKDJ_CROSS1'].values[0] * -1
    SKDJ_MARK_WK = week.iloc[-1:]['SKDJ_CROSS2'].values[0] + week.iloc[-1:]['SKDJ_CROSS1'].values[0] * -1
    return(day_check, week_check, incr, incrs, incr5,incr15,skdj_k,SKDJ_MARK, skdj_k_wk, SKDJ_MARK_WK,mean,per25,per75,perc)

def money_daily(MONEY, date):
    day, week = trends_money(MONEY, date)
    day_check = check(day).loc[MONEY]
    week_check = check(week).loc[MONEY]
    skdj_k = day.iloc[-1:]['SKDJ_K'].values[0]
    skdj_k_wk = week.iloc[-1:]['SKDJ_K'].values[0]
    incr5 = day.iloc[-1:]['MA5'].values[0]
    incr15 = day.iloc[-1:]['MA15'].values[0]
    incr = day.iloc[-1:]['MA15_C'].values[0]
    incrs = day.iloc[-1:]['MA15_D'].values[0]
    mean = day.iloc[-1:]['mean'].values[0]
    per25 = day.iloc[-1:]['per25'].values[0]
    per75 = day.iloc[-1:]['per75'].values[0]
    perc = day.iloc[-1:]['perc'].values[0]
    SKDJ_MARK = day.iloc[-1:]['SKDJ_CROSS2'].values[0] + day.iloc[-1:]['SKDJ_CROSS1'].values[0] * -1
    SKDJ_MARK_WK = week.iloc[-1:]['SKDJ_CROSS2'].values[0] + week.iloc[-1:]['SKDJ_CROSS1'].values[0] * -1
    return(day_check, week_check, incr, incrs,incr5,incr15,skdj_k,SKDJ_MARK,skdj_k_wk,SKDJ_MARK_WK,mean,per25,per75,perc)

def gold_daily(GOLD, date):
    day, week = trends_gold(GOLD, date)
    day_check = check(day).loc[GOLD]
    week_check = check(week).loc[GOLD]
    skdj_k = day.iloc[-1:]['SKDJ_K'].values[0]
    skdj_k_wk = week.iloc[-1:]['SKDJ_K'].values[0]
    incr5 = day.iloc[-1:]['MA5'].values[0]
    incr15 = day.iloc[-1:]['MA15'].values[0]
    incr = day.iloc[-1:]['MA15_C'].values[0]
    incrs = day.iloc[-1:]['MA15_D'].values[0]
    mean = day.iloc[-1:]['mean'].values[0]
    per25 = day.iloc[-1:]['per25'].values[0]
    per75 = day.iloc[-1:]['per75'].values[0]
    perc = day.iloc[-1:]['perc'].values[0]
    SKDJ_MARK = day.iloc[-1:]['SKDJ_CROSS2'].values[0] + day.iloc[-1:]['SKDJ_CROSS1'].values[0] * -1
    SKDJ_MARK_WK = week.iloc[-1:]['SKDJ_CROSS2'].values[0] + week.iloc[-1:]['SKDJ_CROSS1'].values[0] * -1
    return(day_check, week_check, incr, incrs,incr5,incr15,skdj_k,SKDJ_MARK,skdj_k_wk,SKDJ_MARK_WK,mean,per25,per75,perc)

def future_daily(GOLD, date):
    day, week = trends_future(GOLD, date)
    day_check = check(day).loc[GOLD]
    week_check = check(week).loc[GOLD]
    skdj_k = day.iloc[-1:]['SKDJ_K'].values[0]
    skdj_k_wk = week.iloc[-1:]['SKDJ_K'].values[0]
    incr5 = day.iloc[-1:]['MA5'].values[0]
    incr15 = day.iloc[-1:]['MA15'].values[0]
    incr = day.iloc[-1:]['MA15_C'].values[0]
    incrs = day.iloc[-1:]['MA15_D'].values[0]
    mean = day.iloc[-1:]['mean'].values[0]
    per25 = day.iloc[-1:]['per25'].values[0]
    per75 = day.iloc[-1:]['per75'].values[0]
    perc = day.iloc[-1:]['perc'].values[0]
    SKDJ_MARK = day.iloc[-1:]['SKDJ_CROSS2'].values[0] + day.iloc[-1:]['SKDJ_CROSS1'].values[0] * -1
    SKDJ_MARK_WK = week.iloc[-1:]['SKDJ_CROSS2'].values[0] + week.iloc[-1:]['SKDJ_CROSS1'].values[0] * -1
    return(day_check, week_check, incr, incrs,incr5,incr15,skdj_k,SKDJ_MARK,skdj_k_wk,SKDJ_MARK_WK,mean,per25,per75,perc)

def stock_daily(stock, start_date, end_date):
    day, week = trends_stock(stock,start_date,end_date)
    day_check = check(day).loc[stock]
    week_check = check(week).loc[stock]
    skdj_k = day.iloc[-1:]['SKDJ_K'].values[0]
    skdj_k_wk = week.iloc[-1:]['SKDJ_K'].values[0]
    incr5 = day.iloc[-1:]['MA5'].values[0]
    incr15 = day.iloc[-1:]['MA15'].values[0]
    incr = day.iloc[-1:]['MA15_C'].values[0]
    incrs = day.iloc[-1:]['MA15_D'].values[0]
    mean = day.iloc[-1:]['mean'].values[0]
    per25 = day.iloc[-1:]['per25'].values[0]
    per75 = day.iloc[-1:]['per75'].values[0]
    perc = day.iloc[-1:]['perc'].values[0]
    SKDJ_MARK = day.iloc[-1:]['SKDJ_CROSS2'].values[0] + day.iloc[-1:]['SKDJ_CROSS1'].values[0] * -1
    SKDJ_MARK_WK = week.iloc[-1:]['SKDJ_CROSS2'].values[0] + week.iloc[-1:]['SKDJ_CROSS1'].values[0] * -1
    return(day_check, week_check, incr, incrs,incr5, incr15,skdj_k,SKDJ_MARK,skdj_k_wk,SKDJ_MARK_WK,mean,per25,per75,perc)

def globalindex_daily(stock, date):
    day, week = trends_globalindex(stock, date)
    day_check = check(day).loc[stock]
    week_check = check(week).loc[stock]
    skdj_k = day.iloc[-1:]['SKDJ_K'].values[0]
    skdj_k_wk = week.iloc[-1:]['SKDJ_K'].values[0]
    incr5 = day.iloc[-1:]['MA5'].values[0]
    incr15 = day.iloc[-1:]['MA15'].values[0]
    incr = day.iloc[-1:]['MA15_C'].values[0]
    incrs = day.iloc[-1:]['MA15_D'].values[0]
    mean = day.iloc[-1:]['mean'].values[0]
    per25 = day.iloc[-1:]['per25'].values[0]
    per75 = day.iloc[-1:]['per75'].values[0]
    perc = day.iloc[-1:]['perc'].values[0]
    SKDJ_MARK = day.iloc[-1:]['SKDJ_CROSS2'].values[0] + day.iloc[-1:]['SKDJ_CROSS1'].values[0] * -1
    SKDJ_MARK_WK = week.iloc[-1:]['SKDJ_CROSS2'].values[0] + week.iloc[-1:]['SKDJ_CROSS1'].values[0] * -1
    return(day_check, week_check, incr, incrs,incr5, incr15,skdj_k,SKDJ_MARK,skdj_k_wk,SKDJ_MARK_WK,mean,per25,per75,perc)

def stock_hourly(stock, start_date, end_date, date_type):
    hour = trends_stock_hour(stock,start_date,end_date)
    date = end_date + ' ' + date_type
    hour_check = check_hour(hour, date).loc[stock]
    return(hour_check)

def btc_hourly(BTC, end_date, date_type):
    hour = trends_btc_hour(BTC)
    date = end_date + ' ' + date_type
    hour_check = check_hour(hour, date).loc[BTC]
    return(hour_check)