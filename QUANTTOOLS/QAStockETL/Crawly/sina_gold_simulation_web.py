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

def get_gold_day_sina(symbol, date):
    date=date.replace('-','_')
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Connection': 'keep-alive'
               }
    url = 'https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var1=/GlobalFuturesService.getGlobalFuturesDailyKLine?symbol={symbol}&_={date}&source=web'
    options = webdriver.ChromeOptions()
    for (key,value) in headers.items():
        options.add_argument('%s="%s"' % (key, value))
    options.add_argument('headless')
    res = read_data_from_sina(url.format(symbol=symbol, date=date), options)
    data = json.loads('{"result":{"data":'+res.text.split('var1=')[1].replace('(','').replace(');','')+'}}')['result']['data']
    data = pd.DataFrame(data)
    data = data.assign(date = data.date.apply(lambda x:pd.to_datetime(x)))
    data[['open','close','high','low','volume']] = data[['open','close','high','low','volume']].apply(pd.to_numeric)
    if data is None:
        return None
    else:
        return(data)

def get_gold_min_sina(symbol, scala):

    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Connection': 'keep-alive'
               }
    url = 'https://gu.sina.cn/ft/api/jsonp.php/var1=/GlobalService.getMink?symbol={symbol}&type={scala}'
    options = webdriver.ChromeOptions()
    for (key,value) in headers.items():
        options.add_argument('%s="%s"' % (key, value))
    options.add_argument('headless')
    res = read_data_from_sina(url.format(symbol=symbol, scala=scala), options)
    data = json.loads('{"result":{"data":'+res.text.split('var1=')[1].replace('(','').replace(');','')+'}}')['result']['data']
    data = pd.DataFrame(data).rename(columns={'d':'datetime','o':'open','h':'high','l':'low','c':'close','v':'vol'})
    data = data.assign(datetime = data.datetime.apply(lambda x:pd.to_datetime(x)))
    data[['open','close','high','low','vol']] = data[['open','close','high','low','vol']].apply(pd.to_numeric)
    if data is None:
        return None
    else:
        return(data)

if __name__ == '__main__':
    pass