from QUANTTOOLS.Market.MarketReport.JOB.hourly_job import daily_job,index_job
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade

if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        daily_job(mark_day)
        index_job(mark_day)