from QUANTAXIS.QAUtil import  QA_util_log_info
from QUANTTOOLS.message_func.wechat import send_actionnotice


def HOLD(strategy_id, account_info,trading_date, code, name, industry, target_pos, target):
    QA_util_log_info('继续持有 {code}({NAME},{INDUSTRY}), 目标持仓:{target_pos},总金额:{target}====={trading_date}'.format(code=code,NAME= name,
                                                                                                          INDUSTRY=industry,target_pos=target_pos,
                                                                                                          target=target,trading_date=trading_date),
                     ui_log=None)
    send_actionnotice(strategy_id,
                      account_info,
                      '{code}({NAME},{INDUSTRY})====={trading_date}'.format(code=code,NAME= name, INDUSTRY=industry,trading_date=trading_date),
                      direction = 'HOLD',
                      offset='HOLD',
                      volume=abs(target_pos)
                      )