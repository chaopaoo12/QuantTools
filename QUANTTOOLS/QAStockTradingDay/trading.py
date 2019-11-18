import QUANTAXIS as QA
from QUANTTOOLS.message_func.wechat import send_actionnotice
from QUANTTOOLS.QAStockTradingDay.StrategyOne import model, load_model, model_predict, check_model
from QUANTTOOLS.QAStockETL.QAFetch import QA_fetch_stock_fianacial_adv
import pandas as pd
import logging
import strategyease_sdk
from QUANTTOOLS.message_func import build_head, build_table, build_email, send_email
from QUANTTOOLS.QAStockTradingDay.setting import working_dir, yun_ip, yun_port, easytrade_password
from datetime import datetime,timedelta
delta = timedelta(days=6)
delta1 = timedelta(days=1)
delta3 = timedelta(days=7)
delta4 = timedelta(days=8)


def trading(date, strategy_id='机器学习1号', account1='name:client-1', working_dir=working_dir):
    try:
        logging.basicConfig(level=logging.DEBUG)
        client = strategyease_sdk.Client(host=yun_ip, port=yun_port, key=easytrade_password)
        account1=account1
        account_info = client.get_account(account1)
        print(account_info)
        sub_accounts = client.get_positions(account1)['sub_accounts']
    except:
        send_email('错误报告', '云服务器错误,请检查', 'date')

    try:
        model_temp,info_temp = load_model(working_dir = working_dir)
    except:
        send_email('错误报告', '无法正确加载模型,请检查', 'date')

    tar = model_predict(model_temp, str(date[0:7])+"-01",date,info_temp['cols'])
    res = pd.concat([tar[tar['RANK'] <= 5].loc[date][['Z_PROB','O_PROB','RANK']],
                     QA.QAFetch.QA_fetch_get_stock_realtime('tdx',code=list(tar[tar['RANK'] <= 5].loc[date].index)).reset_index('datetime')[['ask1','ask_vol1','bid1','bid_vol1']],
                     QA_fetch_stock_fianacial_adv(list(tar[tar['RANK'] <= 5].loc[date].index),date,date).data.reset_index('date')[['NAME','INDUSTRY']]],
                    axis=1)
    print('计算资金分配')
    avg_account = sub_accounts['可用金额']/tar[tar['RANK'] <= 5].loc[date].shape[0]
    res = res.assign(tar=avg_account[0])
    res['cnt'] = (res['tar']/res['ask1']/100).apply(lambda x:round(x,0)*100)
    res['real'] = res['cnt'] * res['ask1']

    print('近日盈利情况')
    positions = client.get_positions(account1)['positions'][['证券代码','股票余额','可用余额','冻结数量','参考盈亏','盈亏比例(%)','当前持仓']]

    for i in list(positions['证券代码']) + list(res.index):
        mark = res.loc[i]['cnt'] - positions[positions['证券代码'] == i]['当前持仓'].get(0,default=0)
        if mark < -100:
            #卖出mark i
            print('卖出 {code}({NAME},{INDUSTRY}) {cnt}股, 目标持仓:{target},总金额:{tar}'.format(code=i,
                                                                                        NAME= res.loc[i]['NAME'],
                                                                                        INDUSTRY=res.loc[i]['INDUSTRY'],
                                                                                        cnt=abs(mark),
                                                                                        target=res.loc[i]['cnt'],
                                                                                        tar=res.loc[i]['real']))
            try:
                client.sell(account1, symbol=i, type='MARKET', priceType=4, amount=abs(mark)/100)
                send_actionnotice(strategy_id,
                                  account_info,
                                  '{code}({NAME},{INDUSTRY})'.format(code=i,NAME= res.loc[i]['NAME'], INDUSTRY=res.loc[i]['INDUSTRY']),
                                  direction = 'SELL',
                                  offset='OPEN',
                                  volume=abs(mark)
                                  )
            except:
                send_actionnotice(strategy_id,
                                  account_info,
                                  '{code}({NAME},{INDUSTRY}) 交易失败'.format(code=i,NAME= res.loc[i]['NAME'], INDUSTRY=res.loc[i]['INDUSTRY']),
                                  direction = 'SELL',
                                  offset='OPEN',
                                  volume=abs(mark)
                                  )

        elif mark > 100:
            print('买入 {code}({NAME},{INDUSTRY}) {cnt}股, 目标持仓:{target},总金额:{tar}'.format(code=i,
                                                                                        NAME= res.loc[i]['NAME'],
                                                                                        INDUSTRY=res.loc[i]['INDUSTRY'],
                                                                                        cnt=abs(mark),
                                                                                        target=res.loc[i]['cnt'],
                                                                                        tar=res.loc[i]['real']))
            try:
                client.buy(account1, symbol=i, type='MARKET', priceType=4, amount=abs(mark)/100)
                send_actionnotice(strategy_id,
                                  account_info,
                                  '{code}({NAME},{INDUSTRY})'.format(code=i,NAME= res.loc[i]['NAME'], INDUSTRY=res.loc[i]['INDUSTRY']),
                                  direction = 'BUY',
                                  offset='OPEN',
                                  volume=abs(mark)
                                  )
            except:
                send_actionnotice(strategy_id,
                                  account_info,
                                  '{code}({NAME},{INDUSTRY}) 交易失败'.format(code=i,NAME= res.loc[i]['NAME'], INDUSTRY=res.loc[i]['INDUSTRY']),
                                  direction = 'BUY',
                                  offset='OPEN',
                                  volume=abs(mark)
                                  )
        elif mark >= -100 and mark <= 100:
            print('继续持有 {code}({NAME},{INDUSTRY}), 目标持仓:{target},总金额:{tar}'.format(code=i,
                                                                                   NAME= res.loc[i]['NAME'],
                                                                                   INDUSTRY=res.loc[i]['INDUSTRY'],
                                                                                   target=res.loc[i]['cnt'],
                                                                                   tar=res.loc[i]['real']))
            send_actionnotice(strategy_id,
                              account_info,
                              '{code}({NAME},{INDUSTRY})'.format(code=i,NAME= res.loc[i]['NAME'], INDUSTRY=res.loc[i]['INDUSTRY']),
                              direction = 'HOLD',
                              offset='HOLD',
                              volume=abs(mark)
                              )
        else:
            pass


