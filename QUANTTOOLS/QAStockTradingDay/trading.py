from QUANTAXIS.QAFetch import QA_fetch_get_stock_realtime
from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTTOOLS.QAStockTradingDay.StrategyOne import load_model, model_predict
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_fianacial_adv
import pandas as pd
import logging
import strategyease_sdk
from QUANTTOOLS.message_func import send_email
from QUANTTOOLS.QAStockTradingDay.setting import working_dir, yun_ip, yun_port, easytrade_password
from QUANTAXIS.QAUtil import QA_util_log_info

def trading(date, strategy_id= '机器学习1号', account1= 'name:client-1', working_dir= working_dir, ui_log= None):
    try:
        QA_util_log_info(
            '##JOB01 Now Got Account Info ==== {}'.format(str(date)), ui_log)
        logging.basicConfig(level=logging.DEBUG)
        client = strategyease_sdk.Client(host=yun_ip, port=yun_port, key=easytrade_password)
        account1=account1
        account_info = client.get_account(account1)
        print(account_info)
        sub_accounts = client.get_positions(account1)['sub_accounts']
    except:
        send_email('错误报告', '云服务器错误,请检查', 'date')
        send_actionnotice(strategy_id,
                          '错误报告:{}'.format(date),
                          '云服务器错误,请检查',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )

    try:
        QA_util_log_info(
            '##JOB02 Now Load Model ==== {}'.format(str(date)), ui_log)
        model_temp,info_temp = load_model(working_dir = working_dir)
    except:
        send_email('错误报告', '无法正确加载模型,请检查', 'date')
        send_actionnotice(strategy_id,
                          '错误报告:{}'.format(date),
                          '无法正确加载模型,请检查',
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=None
                          )

    QA_util_log_info(
        '##JOB03 Now Model Predict ==== {}'.format(str(date)), ui_log)
    tar = model_predict(model_temp, str(date[0:7])+"-01",date,info_temp['cols'])

    QA_util_log_info(
        '##JOB04 Now Current Holding ==== {}'.format(str(date)), ui_log)
    positions = client.get_positions(account1)['positions'][['证券代码','证券名称','股票余额','可用余额','冻结数量','参考盈亏','盈亏比例(%)','当前持仓']]

    r1 = pd.concat([tar[tar['RANK'] <= 5].loc[date][['Z_PROB','O_PROB','RANK']],
                    positions.set_index('证券代码')],axis=1)
    r1['当前持仓'] = r1['当前持仓'].fillna(0)

    QA_util_log_info(
        '##JOB05 Now Concat Result ==== {}'.format(str(date)), ui_log)
    res = pd.concat([r1,
                     QA_fetch_get_stock_realtime('tdx', code=list(r1.index)).reset_index('datetime')[['ask1','ask_vol1','bid1','bid_vol1']],
                     QA_fetch_stock_fianacial_adv(list(r1.index), date, date).data.reset_index('date')[['NAME','INDUSTRY']]],
                    axis=1)

    QA_util_log_info(
        '##JOB06 Now Funding Decision ==== {}'.format(str(date)), ui_log)
    avg_account = sub_accounts['总 资 产']/tar[tar['RANK'] <= 5].loc[date].shape[0]
    res = res.assign(tar=avg_account[0])
    res.ix[res['RANK'].isnull(),'tar'] = 0
    res['cnt'] = (res['tar']/res['ask1']/100).apply(lambda x: round(x, 0)*100)
    res['real'] = res['cnt'] * res['ask1']
    res = res.sort_values(by='ask1', ascending= False)
    res.ix[-1, 'cnt'] = round((res['real'][-1]-(res['real'].sum()-res['tar'].sum()))/res['ask1'][-1]/100, 0)*100
    res['real'] = res['cnt'] * res['ask1']
    res['mark'] = res['cnt'] - res['当前持仓'].apply(lambda x:float(x))

    QA_util_log_info(
        '##JOB06 Now Trading ==== {}'.format(str(date)), ui_log)

    for i in res[res['mark'] < 0].index:
        cnt = float(res.get_value(i, 'cnt'))
        tar = float(res.get_value(i, 'real'))
        NAME = res.get_value(i, 'NAME')
        INDUSTRY = res.get_value(i, 'INDUSTRY')
        mark = abs(float(res.get_value(i, 'mark')))

        print('卖出 {code}({NAME},{INDUSTRY}) {cnt}股, 目标持仓:{target},总金额:{tar}'.format(code=i,
                                                                                    NAME= NAME,
                                                                                    INDUSTRY= INDUSTRY,
                                                                                    cnt=abs(mark),
                                                                                    target=cnt,
                                                                                    tar=tar))
        try:
            client.sell(account1, symbol=i, type='MARKET', priceType=4, amount=abs(mark))
            send_actionnotice(strategy_id,
                              account_info,
                              '{code}({NAME},{INDUSTRY})'.format(code=i,NAME= NAME, INDUSTRY=INDUSTRY),
                              direction = 'SELL',
                              offset='OPEN',
                              volume=abs(mark)
                              )
        except:
            send_actionnotice(strategy_id,
                              account_info,
                              '{code}({NAME},{INDUSTRY}) 交易失败'.format(code=i,NAME= NAME, INDUSTRY=INDUSTRY),
                              direction = 'SELL',
                              offset='OPEN',
                              volume=abs(mark)
                              )

    for i in res[res['mark'] == 0].index:
        cnt = float(res.get_value(i, 'cnt'))
        tar = float(res.get_value(i, 'real'))
        NAME = res.get_value(i, 'NAME')
        INDUSTRY = res.get_value(i, 'INDUSTRY')
        mark = abs(float(res.get_value(i, 'mark')))
        print('继续持有 {code}({NAME},{INDUSTRY}), 目标持仓:{target},总金额:{tar}'.format(code=i,
                                                                               NAME= NAME,
                                                                               INDUSTRY=INDUSTRY,
                                                                               target=cnt,
                                                                               tar=tar))
        send_actionnotice(strategy_id,
                          account_info,
                          '{code}({NAME},{INDUSTRY})'.format(code=i,NAME= NAME, INDUSTRY=INDUSTRY),
                          direction = 'HOLD',
                          offset='HOLD',
                          volume=abs(mark)
                          )

    for i in res[res['mark'] > 0].index:
        cnt = float(res.get_value(i, 'cnt'))
        tar = float(res.get_value(i, 'real'))
        NAME = res.get_value(i, 'NAME')
        INDUSTRY = res.get_value(i, 'INDUSTRY')
        mark = abs(float(res.get_value(i, 'mark')))
        print('买入 {code}({NAME},{INDUSTRY}) {cnt}股, 目标持仓:{target},总金额:{tar}'.format(code=i,
                                                                                    NAME= NAME,
                                                                                    INDUSTRY=INDUSTRY,
                                                                                    cnt=abs(mark),
                                                                                    target=cnt,
                                                                                    tar=tar))
        try:
            client.buy(account1, symbol=i, type='MARKET', priceType=4, amount=abs(mark))
            send_actionnotice(strategy_id,
                              account_info,
                              '{code}({NAME},{INDUSTRY})'.format(code=i,NAME= NAME, INDUSTRY=INDUSTRY),
                              direction = 'BUY',
                              offset='OPEN',
                              volume=abs(mark)
                              )
        except:
            send_actionnotice(strategy_id,
                              account_info,
                              '{code}({NAME},{INDUSTRY}) 交易失败'.format(code=i,NAME= NAME, INDUSTRY=INDUSTRY),
                              direction = 'BUY',
                              offset='OPEN',
                              volume=abs(mark)
                              )


