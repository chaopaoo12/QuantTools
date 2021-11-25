from QUANTTOOLS.Market.MarketTools.TrackTools.track import track_roboot3
from QUANTAXIS.QAUtil import QA_util_log_info


def tracking_base(trading_date, strategy_id, account, exceptions):


    QA_util_log_info('##JOB## Now Tracking ===== {}'.format(str(trading_date)))

    res = track_roboot3(account, trading_date, strategy_id, exceptions = exceptions)

    return(res)

if __name__ == '__main__':
    pass