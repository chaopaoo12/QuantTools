from QUANTTOOLS.Market.MarketTools.predict_tools import save_prediction, prediction_report

def predict_base(trading_date, predict_func, model_name, file_name, top_num, percent, working_dir, exceptions):
    stock_tar, stock_b, start, end, model_date = predict_func(trading_date, model_name = model_name,  working_dir=working_dir)
    save_prediction({'date': trading_date, 'target_pool':stock_tar, 'prediction':stock_b}, file_name, working_dir)
    prediction_report(trading_date, stock_tar, stock_b, model_date, top_num, exceptions, percent, account='name:client-1', ui_log = None)