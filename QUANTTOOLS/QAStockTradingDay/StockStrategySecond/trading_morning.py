from QUANTTOOLS.message_func import send_email
from QUANTTOOLS.account_manage import trade_roboot
from QUANTTOOLS.QAStockTradingDay.StockStrategySecond.setting import working_dir, percent, exceptions
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.account_manage import get_Client,check_Client
import time
import datetime
from QUANTTOOLS.QAStockTradingDay.StockStrategySecond.concat_predict import concat_predict,load_prediction,check_prediction

def trading(trading_date, percent=percent, strategy_id= '机器学习1号', account= 'name:client-1', working_dir= working_dir, ui_log= None, exceptions= exceptions):

    QA_util_log_info('##JOB01 Now Predict ==== {}'.format(str(trading_date)), ui_log)
    try:
        prediction = load_prediction('prediction', working_dir)
        check_prediction(prediction, trading_date)
        tar = prediction['tar']
    except:
        tar,index_tar,safe_tar,stock_tar,start,end,model_date = concat_predict(trading_date, strategy_id=strategy_id,  working_dir=working_dir)

    try:
        r_tar = tar.loc[trading_date][['Z_PROB','O_PROB','RANK']]
    except:
        r_tar = None
        send_email('交易报告:'+ trading_date, "空仓状态", 'date')

    QA_util_log_info(
        '##JOB03 Now Chect Account Server ==== {}'.format(str(trading_date)), ui_log)
    client = get_Client()
    check_Client(client, account, strategy_id, trading_date, exceptions=exceptions)

    h1 = int(datetime.datetime.now().strftime("%H"))
    m1 = int(datetime.datetime.now().strftime("%M"))
    while h1 == 9 and m1 <= 29 :
        h1 = int(datetime.datetime.now().strftime("%H"))
        m1 = int(datetime.datetime.now().strftime("%M"))
        time.sleep(30)

    QA_util_log_info(
        '##JOB04 Now Trading ==== {}'.format(str(trading_date)), ui_log)
    res = trade_roboot(r_tar, account, trading_date, percent, strategy_id, type='morning', exceptions = exceptions)
    return(res)


