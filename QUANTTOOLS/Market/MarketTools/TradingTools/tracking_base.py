from QUANTTOOLS.Market.MarketTools.TradingTools import load_data
from QUANTTOOLS.Market.MarketTools.TradingTools.track import track_roboot,track_roboot2,track_roboot3
from QUANTAXIS.QAUtil import QA_util_log_info, QA_util_get_last_day
import time
import datetime

def tracking_base(trading_date, strategy_id, account, exceptions):


    QA_util_log_info('##JOB## Now Tracking ===== {}'.format(str(trading_date)))

    res = track_roboot3(account, trading_date, strategy_id, exceptions = exceptions)

    return(res)

if __name__ == '__main__':
    pass