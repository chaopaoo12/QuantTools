#查询账户总资产
import logging
import strategyease_sdk
from QUANTAXIS import QA_fetch_get_stock_realtime
logging.basicConfig(level=logging.DEBUG)

client = strategyease_sdk.Client(host='132.232.89.97', port=8888, key='123456')
account1='name:client-1'
account_info = client.get_account(account1)
sub_accounts = client.get_positions(account1)['sub_accounts']

#计算资金分配
tar = None
avg_account = sub_accounts['可用金额']/tar[tar['RANK'] <= 5].loc['2019-08-20'].shape[0]
target = QA_fetch_get_stock_realtime('tdx',list(tar[tar['RANK'] <= 5].loc['2019-08-20'].index))
target['cnt'] = target['last_close'].apply(lambda x:round(avg_account/x/100,0)*100)
target['target_market'] = target['cnt'] * target['last_close']

positions = client.get_positions(account1)['positions']
positions['证券代码','证券名称','可用余额','参考盈亏','成本价','盈亏比例(%)','当前持仓']

##计算历史盈亏


##执行与跟踪
#调仓操作
#输入 a 目前持仓 目前市值 单价 数量
#     b 目标持仓 目标金额 目标数量
#操作细节 拆单 每单不超过10W

def rebalance_account(real, target):
    #for i in real.key:
    #   if target[i] is None
    pass

#调仓报告
#汇报卖方操作盈亏情况
positions = client.get_positions(account1)['positions']
positions['证券代码','证券名称','可用余额','参考盈亏','成本价','盈亏比例(%)','当前持仓']

#汇报买方操作标的情况
