from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTTOOLS.message_func import send_email
from QUANTTOOLS.account_manage import trade_roboot
from QUANTTOOLS.QAStockTradingDay.StockStrategySecond.setting import working_dir, percent, exceptions
from QUANTAXIS.QAUtil import QA_util_log_info
from QUANTTOOLS.account_manage import get_Client
from QUANTTOOLS.QAStockTradingDay.StockStrategySecond.concat_predict import concat_predict,load_prediction,check_prediction
import time
import datetime

def trading(trading_date, percent=percent, strategy_id= '机器学习1号', account1= 'name:client-1', working_dir= working_dir, ui_log= None, exceptions= exceptions):

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

    try:
        QA_util_log_info(
            '##JOB03 Now Chect Account Server ==== {}'.format(str(trading_date)), ui_log)
        client = get_Client()
        account1=account1
        client.cancel_all(account1)
        account_info = client.get_account(account1)
        print(account_info)
    except:
        send_email('错误报告', '云服务器错误,请检查', trading_date)
        send_actionnotice(strategy_id,
                          '错误报告:{}'.format(trading_date),
                          '云服务器错误,请检查',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )

    send_actionnotice(strategy_id,
                      '交易报告:{}'.format(trading_date),
                      '交易准备已完成',
                      direction = 'HOLD',
                      offset='HOLD',
                      volume=None
                      )

    '##JOB04 Now Timing Control ==== {}'
    h1 = int(datetime.datetime.now().strftime("%H"))
    m1 = int(datetime.datetime.now().strftime("%M"))
    while h1 == 14 and m1 <= 50 :
        h1 = int(datetime.datetime.now().strftime("%H"))
        m1 = int(datetime.datetime.now().strftime("%M"))
        time.sleep(15)

    QA_util_log_info(
        '##JOB04 Now Trading ==== {}'.format(str(trading_date)), ui_log)
    res = trade_roboot(r_tar, account1, trading_date, percent, strategy_id, type='end', exceptions = exceptions)
    return(res)

if __name__ == '__main__':
    pass
