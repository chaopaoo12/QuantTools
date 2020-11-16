import pandas as pd
import requests
import json

def read_data_ifzq(code, ex, type):
    url = 'http://ifzq.gtimg.cn/appstock/app/kline/mkline?param={ex}{code},{type},,800&_var=m60_today'
    response = requests.get(url.format(ex=ex,code=code,type=type))
    data = json.loads(response.text.split('=')[1])
    res = pd.DataFrame(data['data'][ex+code]['m60'], columns = ['datetime','open','close','high','low','volume','aaa','bbb'])
    res[['open','close','high','low','volume']] = res[['open','close','high','low','volume']].apply(pd.to_numeric)
    res = res.assign(date=res.datetime.apply(lambda x: x[0:4]+'-'+x[4:6]+'-'+x[6:8]),
                     datetime=res.datetime.apply(lambda x: x[0:4]+'-'+x[4:6]+'-'+x[6:8]+' '+x[8:10]+':'+x[10:12]+':00'),
                     code=code)
    return(res)
