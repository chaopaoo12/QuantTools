from QUANTTOOLS.QAStockTradingDay.daily_job import trading
from  QUANTAXIS.QAUtil import QA_util_today_str
mark_day = QA_util_today_str()
trading(mark_day)