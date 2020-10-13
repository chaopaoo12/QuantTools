from QUANTTOOLS.Market.MarketTools import load_prediction,check_prediction
from QUANTTOOLS.Message import send_email
from QUANTAXIS.QAUtil import QA_util_log_info

def load_data(predict_func, trading_date, working_dir, model_name, file_name):
    QA_util_log_info('##JOB## Now Predict ==== {}'.format(str(trading_date)))
    try:
        prediction = load_prediction(file_name, working_dir)
        check_prediction(prediction, trading_date)
        target_pool = prediction['target_pool']
        prediction = prediction['prediction']
    except:
        target_pool, prediction, start, end, model_date = predict_func(trading_date, model_name=model_name,  working_dir=working_dir)

    try:
        r_tar = target_pool.loc[trading_date][['NAME','INDUSTRY','Z_PROB','O_PROB','RANK']]
        prediction_tar = prediction.loc[trading_date][['NAME','INDUSTRY','Z_PROB','O_PROB','RANK']]
    except:
        r_tar = None
        prediction_tar =  None
        send_email('交易报告:'+ trading_date, "空仓状态", 'date')
    return(r_tar, prediction_tar)