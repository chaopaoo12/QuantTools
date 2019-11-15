import requests
import datetime
from QUANTTOOLS.message_func.setting import QAPRO_ID

# 下面的参数自行填写
# user, strategy_id, account, code, direction, offset, price, volume, 

def send_actionnotice(strategy_id,
                      account,
                      code,
                      name,
                      industry,
                      direction,
                      offset,
                      price,
                      volume,
                      user = QAPRO_ID,
                      now = str(datetime.datetime.now)):
    requests.post("http://www.yutiansut.com/signal?user_id={}&template=xiadan_report&strategy_id={}&realaccount={}&code={}&order_direction={}&order_offset={}&price={}&volume={}&order_time={}".format(user = user, strategy_id = strategy_id, account = account, code = code, direction= direction, offset= offset, price = price, volume = volume, now =now))