import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
import json

#BTC btcbtcusd
#比特币美元现金 btcbchusd
#https://quotes.sina.cn/fx/api/openapi.php/BtcService.getDayKLine?symbol={symbol}&callback=var1=
#https://quotes.sina.cn/fx/api/openapi.php/BtcService.getMinKline?symbol={symbol}&scale={scala}&datalen={lens}&callback=var1=

def read_data_from_sina(url, options):
    driver = webdriver.Chrome(chrome_options=options)
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, "html.parser").body
    driver.quit()
    return(soup)

def get_btc_day_sina(symbol):

    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Connection': 'keep-alive'
               }
    url = 'https://quotes.sina.cn/fx/api/openapi.php/BtcService.getDayKLine?symbol={symbol}&callback=var1='
    options = webdriver.ChromeOptions()
    for (key,value) in headers.items():
        options.add_argument('%s="%s"' % (key, value))
    options.add_argument('headless')
    res = read_data_from_sina(url.format(symbol=symbol), options)
    data = json.loads(res.text.split('var1=')[1].replace('(','').replace(')',''))['result']['data']
    data = pd.DataFrame([i.split(',') for i in data.split('|')], columns = ['date','open','low','high','close','vol'])
    if data is None:
        return None
    else:
        return(data)

def get_btc_min_sina(symbol, scala, lens):

    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Connection': 'keep-alive'
               }
    url = 'https://quotes.sina.cn/fx/api/openapi.php/BtcService.getMinKline?symbol={symbol}&scale={scala}&datalen={lens}&callback=var1='
    options = webdriver.ChromeOptions()
    for (key,value) in headers.items():
        options.add_argument('%s="%s"' % (key, value))
    options.add_argument('headless')
    res = read_data_from_sina(url.format(symbol=symbol, scala=scala, lens=lens), options)
    data = json.loads(res.text.split('var1')[1].replace('(','').replace(')',''))['result']['data']
    data = pd.DataFrame(data).rename(columns={'d':'datetime','o':'open','h':'high','l':'low','c':'close','v':'vol'})
    if data is None:
        return None
    else:
        return(data)