from QUANTTOOLS.Market.MarketTools import load_prediction,check_prediction
from QUANTTOOLS.Message import send_email
from QUANTAXIS.QAUtil import QA_util_log_info

def load_data(trading_date, working_dir, file_name):
    QA_util_log_info('##JOB## Now Predict ==== {}'.format(str(trading_date)))
    try:
        prediction = load_prediction(file_name, working_dir)
        check_prediction(prediction, trading_date)
        target_pool = prediction['target_pool']
        prediction = prediction['prediction']
        print(target_pool)
        print(prediction)
    except:
        pass

    try:
        r_tar = target_pool.loc[trading_date]
        prediction_tar = prediction.loc[trading_date]
    except:
        r_tar = None
        prediction_tar =  None
        send_email('交易报告:'+ trading_date, "空仓状态", 'date')
    return(r_tar, prediction_tar)