import logging
import strategyease_sdk
from QUANTTOOLS.message_func.email import send_email

logging.basicConfig(level=logging.DEBUG)
client = strategyease_sdk.Client(host='132.232.89.97', port=8888, key='123456')

account1='name:client-1'
try:
    account_info = client.get_account(account1)
    print(account_info)
except:
    print('no pass')
    send_email("warning 云账户登录失败")

#to_do
#自动打新
try:
    client.purchase_new_stocks(account1)
    send_email("warning 打新成功")
except:
    print('打新失败')
    send_email("warning 打新失败")

