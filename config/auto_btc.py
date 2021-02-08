from QUANTTOOLS.Market.MarketReport.JOB.hourly_job import auto_btc_tracking
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_get_last_day,QA_util_get_real_date,QA_util_if_trade


if __name__ == '__main__':
    if QA_util_get_last_day(QA_util_today_str()) == 'wrong date':
        mark_day = QA_util_get_real_date(QA_util_today_str())
    else:
        mark_day = QA_util_get_last_day(QA_util_today_str())

    if QA_util_if_trade(QA_util_today_str()):
        auto_btc_tracking(mark_day)