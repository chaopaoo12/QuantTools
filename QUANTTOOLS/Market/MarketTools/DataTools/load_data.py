from QUANTTOOLS.Market.MarketTools import load_prediction,check_prediction
from QUANTTOOLS.Message import send_email
from QUANTAXIS.QAUtil import QA_util_log_info

def load_data(func, trading_date, working_dir, model_name, file_name):
    QA_util_log_info('##JOB Now Predict ==== {}'.format(str(trading_date)))
    try:
        prediction = load_prediction(file_name, working_dir)
        check_prediction(prediction, trading_date)
        target_pool = prediction['target_pool']
        prediction_tar = prediction['prediction']
    except:
        target_pool,prediction,start,end,Model_Date, model_name, target = func(trading_date, working_dir=working_dir, model_name=model_name)
        target_pool = target_pool
        prediction_tar = prediction
    try:
        r_tar = target_pool.loc[trading_date]
    except:
        r_tar = None
        #prediction_tar =  None
        send_email('交易报告:'+ trading_date, "空仓状态", 'date')
    return(r_tar, prediction_tar, prediction)


if __name__ == '__main__':
    pass