import requests
import datetime
from QUANTTOOLS.Message.message_func.setting import QAPRO_ID

# 下面的参数自行填写
# user, strategy_id, account, code, direction, offset, price, volume, 

def send_actionnotice(strategy_id,
                      account,
                      code,
                      direction,
                      offset,
                      volume,
                      price=None,
                      user = QAPRO_ID,
                      now = str(datetime.datetime.now())):
    try:
        requests.post("http://www.yutiansut.com/signal?user_id={user}&template=xiadan_report&strategy_id={strategy_id}&realaccount={account}&code={code}&order_direction={direction}&order_offset={offset}&price={price}&volume={volume}&order_time={now}".format(user = user, strategy_id = strategy_id, account = account, code = code, direction= direction, offset= offset, price = price, volume = volume, now =now))
    except:
        pass