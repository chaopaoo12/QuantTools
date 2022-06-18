from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_if_trade
from QUANTTOOLS.Market.MarketReport.JOB.daily_job import aotu_bond


if __name__ == '__main__':
    mark_day = QA_util_today_str()

    if QA_util_if_trade(mark_day):
        aotu_bond(mark_day)
