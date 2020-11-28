import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
import json

#主要品种
#美元指数 DINIW
#美元人民币 USDCNY
#美元日元 USDJPY
#美元欧元 USDEUR
#美元英镑 USDGBP
#https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var1=/NewForexService.getMinKline?symbol=fx_s{symbol}&scale={scale}&datalen={lens}
#日K
#https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var1=/NewForexService.getDayKLine?symbol=fx_seurusd&_=2020_11_28
def read_data_from_sina(url, options):
    driver = webdriver.Chrome(chrome_options=options)
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, "html.parser").body
    driver.quit()
    return(soup)

def get_money_day_sina(symbol, date):
    date=date.replace('-','_')
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Connection': 'keep-alive'
               }
    url = 'https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var1=/NewForexService.getDayKLine?symbol=fx_s{symbol}&_={date}'
    options = webdriver.ChromeOptions()
    for (key,value) in headers.items():
        options.add_argument('%s="%s"' % (key, value))
    options.add_argument('headless')
    res = read_data_from_sina(url.format(symbol=symbol, date=date), options)
    data = pd.DataFrame([i.split(',') for i in res.text.split('var1=')[1].replace('("','').replace('");','').split(',|')], columns = ['date','open','low','high','close'])
    data = data.assign(date = data.date.apply(lambda x:pd.to_datetime(x)))
    data[['open','close','high','low']] = data[['open','close','high','low']].apply(pd.to_numeric)
    if data is None:
        return None
    else:
        return(data)

def get_money_min_sina(symbol, scala, lens):

    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Connection': 'keep-alive'
               }
    url = 'https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var1=/NewForexService.getMinKline?symbol=fx_s{symbol}&scale={scala}&datalen={lens}'
    options = webdriver.ChromeOptions()
    for (key,value) in headers.items():
        options.add_argument('%s="%s"' % (key, value))
    options.add_argument('headless')
    res = read_data_from_sina(url.format(symbol=symbol, scala=scala, lens=lens), options)
    data = json.loads('{"result":{"data":'+res.text.split('var1=')[1].replace('(','').replace(');','')+'}}')['result']['data']
    data = pd.DataFrame(data).rename(columns={'d':'datetime','o':'open','h':'high','l':'low','c':'close'})
    data = data.assign(datetime = data.datetime.apply(lambda x:pd.to_datetime(x)))
    data[['open','close','high','low']] = data[['open','close','high','low']].apply(pd.to_numeric)
    if data is None:
        return None
    else:
        return(data)

def get_diniw_min_sina(scala, lens):

    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Connection': 'keep-alive'
               }
    url = 'https://vip.stock.finance.sina.com.cn/forex/api/jsonp.php/var1=/NewForexService.getOldMinKline?symbol=DINIW&scale={scala}&datalen={lens}'
    options = webdriver.ChromeOptions()
    for (key,value) in headers.items():
        options.add_argument('%s="%s"' % (key, value))
    options.add_argument('headless')
    res = read_data_from_sina(url.format(scala=scala, lens=lens), options)
    data = json.loads('{"result":{"data":'+res.text.split('var1=')[1].replace('(','').replace(');','')+'}}')['result']['data']
    data = pd.DataFrame(data).rename(columns={'d':'datetime','o':'open','h':'high','l':'low','c':'close','v':'vol'})
    data = data.assign(datetime = data.datetime.apply(lambda x:pd.to_datetime(x)))
    data[['open','close','high','low']] = data[['open','close','high','low']].apply(pd.to_numeric)
    if data is None:
        return None
    else:
        return(data)