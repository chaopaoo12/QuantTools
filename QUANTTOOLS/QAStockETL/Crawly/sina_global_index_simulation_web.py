import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
import json

#纽约黄金 GC
#分时
#https://gu.sina.cn/ft/api/jsonp.php/var1=/GlobalService.getMink?symbol={symbol}&type={scala}
#日K
#https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var1=/GlobalFuturesService.getGlobalFuturesDailyKLine?symbol={symbol}&_={date}&source=web
#
#伦敦黄金 XAU
#分时
#https://gu.sina.cn/ft/api/jsonp.php/var1=/GlobalService.getMink?symbol={symbol}&type={scala}
#日K
#https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var1=/GlobalFuturesService.getGlobalFuturesDailyKLine?symbol=XAU&_=2020_11_28&source=web

def read_data_from_sina(url, options):
    driver = webdriver.Chrome(chrome_options=options)
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, "html.parser").body
    driver.quit()
    return(soup)

def get_globalindex_day_sina(symbol):
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Connection': 'keep-alive'
               }
    url = 'https://stock.finance.sina.com.cn/usstock/api/jsonp.php/var1=/Global_IndexService.getDayLine?symbol={symbol}&num=3000'
    options = webdriver.ChromeOptions()
    for (key,value) in headers.items():
        options.add_argument('%s="%s"' % (key, value))
    options.add_argument('headless')
    res = read_data_from_sina(url.format(symbol=symbol), options)
    data = json.loads('{"result":{"data":'+res.text.split('var1=')[1].replace('(','').replace(');','')+'}}')['result']['data']
    data = pd.DataFrame(data)
    data[['open','close','high','low','volume']] = data[['o','c','h','l','v']].apply(pd.to_numeric)
    data = data.assign(date = data.d.apply(lambda x:pd.to_datetime(x)),
                       code = symbol)
    if data is None:
        return None
    else:
        return(data)

def get_InnerFut_day_sina(symbol, date):
    date=date.replace('-','_')
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Connection': 'keep-alive'
               }
    url = 'https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var1=/InnerFuturesNewService.getDailyKLine?symbol={symbol}&_={date}'
    options = webdriver.ChromeOptions()
    for (key,value) in headers.items():
        options.add_argument('%s="%s"' % (key, value))
    options.add_argument('headless')
    res = read_data_from_sina(url.format(symbol=symbol,date=date), options)
    data = json.loads('{"result":{"data":'+res.text.split('var1=')[1].replace('(','').replace(');','')+'}}')['result']['data']
    data = pd.DataFrame(data)
    data[['open','close','high','low','volume']] = data[['o','c','h','l','v']].apply(pd.to_numeric)
    data = data.assign(date = data.d.apply(lambda x:pd.to_datetime(x)),
                       code = symbol)
    if data is None:
        return None
    else:
        return(data)

if __name__ == '__main__':
    pass