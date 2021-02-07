from QUANTTOOLS.Market.MarketTools.trading_tools import load_data
from QUANTTOOLS.Market.MarketTools.trading_tools.track import track_roboot
from QUANTAXIS.QAUtil import QA_util_log_info
import time
import datetime

def tracking_base(trading_date, strategy_id, func, model_name, file_name, percent, account, working_dir, exceptions):

    r_tar, prediction_tar, prediction = load_data(func, trading_date, working_dir, model_name, file_name)
    r_tar = prediction_tar[(prediction_tar.RANK <= 20)&(prediction_tar.TARGET5.isnull())].reset_index(level=0, drop=True).drop_duplicates(subset='NAME')

    QA_util_log_info('##JOB## Now Tracking ===== {}'.format(str(trading_date)))

    res = track_roboot(r_tar, account, trading_date, percent, strategy_id, exceptions = exceptions)
    return(res)

if __name__ == '__main__':
    pass