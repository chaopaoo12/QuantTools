
from QUANTTOOLS.Ananlysis.Trends.setting import BTC, GOLD, MONEY, CN_INDEX, US_INDEX, FUTURE
from QUANTTOOLS.Ananlysis.Trends.trends import btc_daily, money_daily, gold_daily, stock_daily, stock_hourly
import pandas as pd
from QUANTTOOLS.Message import build_head, build_table, build_email, send_email, send_actionnotice

def aotu_report(trading_date):
    BTC_RES = pd.DataFrame(columns=('code', 'daily', 'weekly'))
    for code in BTC:
        res = btc_daily(code)
        BTC_RES = BTC_RES.append({'code':code, 'daily':res[0], 'weekly':res[1], 'speed':res[2], 'chg':res[3]}, ignore_index=True)

    for code in GOLD:
        res = gold_daily(code, trading_date)
        BTC_RES = BTC_RES.append({'code':code, 'daily':res[0], 'weekly':res[1], 'speed':res[2], 'chg':res[3]}, ignore_index=True)

    for code in MONEY:
        res = money_daily(code, trading_date)
        BTC_RES = BTC_RES.append({'code':code, 'daily':res[0], 'weekly':res[1], 'speed':res[2], 'chg':res[3]}, ignore_index=True)

    for code in CN_INDEX:
        res = stock_daily(code, trading_date, trading_date)
        BTC_RES = BTC_RES.append({'code':code, 'daily':res[0], 'weekly':res[1], 'speed':res[2], 'chg':res[3]}, ignore_index=True)

    for code in US_INDEX:
        res = stock_daily(code, trading_date, trading_date)
        BTC_RES = BTC_RES.append({'code':code, 'daily':res[0], 'weekly':res[1], 'speed':res[2], 'chg':res[3]}, ignore_index=True)
    target_body = build_table(BTC_RES, '目标持仓')
    msg = build_email(build_head(),target_body)
    send_email('金融产品价格趋势' + trading_date, msg, trading_date)
    return(BTC_RES)