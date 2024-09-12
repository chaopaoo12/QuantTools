import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
import json

#BTC btcbtcusd
#比特币美元现金 btcbchusd
#https://quotes.sina.cn/fx/api/openapi.php/BtcService.getDayKLine?symbol={symbol}&callback=var1=
#https://quotes.sina.cn/fx/api/openapi.php/BtcService.getMinKline?symbol={symbol}&scale={scala}&datalen={lens}&callback=var1=

def read_data_from_sina(url, options):
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, "html.parser").body
    driver.close()
    return(soup)

def get_btc_day_sina(symbol):

    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Cache-Control': 'max-age=0',
               'Connection': 'keep-alive'
               }
    url = 'https://quotes.sina.cn/fx/api/openapi.php/BtcService.getDayKLine?symbol={symbol}'
    options = webdriver.ChromeOptions()
    for (key,value) in headers.items():
        options.add_argument('%s="%s"' % (key, value))
    options.page_load_strategy = 'none'
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument('--headless')
    options.add_argument('–-disable-javascript')   #禁用javascript
    options.add_argument('--disable-plugins')   #禁用插件
    options.add_argument("--disable--gpu")#禁用显卡
    options.add_argument("--disable-images")#禁用图像
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument('ignore-certificate-errors')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    res = read_data_from_sina(url.format(symbol=symbol), options)
    data = pd.DataFrame([i.split(',') for i in res.text.split('"data":"')[1].split('"}}')[0].split('|')], 
                        columns = ['date','open','low','high','close','vol','amount'])
    data = data.assign(date = data.date.apply(lambda x:pd.to_datetime(x)))
    data[['open','close','high','low','vol','amount']] = data[['open','close','high','low','vol','amount']].apply(pd.to_numeric)
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
    url = 'https://quotes.sina.cn/fx/api/openapi.php/BtcService.getMinKline?symbol={symbol}&scale={scala}&datalen={lens}'
    options = webdriver.ChromeOptions()
    for (key,value) in headers.items():
        options.add_argument('%s="%s"' % (key, value))
    options.page_load_strategy = 'none'
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument('--headless')
    options.add_argument('–-disable-javascript')   #禁用javascript
    options.add_argument('--disable-plugins')   #禁用插件
    options.add_argument("--disable--gpu")#禁用显卡
    options.add_argument("--disable-images")#禁用图像
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument('ignore-certificate-errors')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    res = read_data_from_sina(url.format(symbol=symbol, scala=scala, lens=lens), options)
    data = json.loads(res.text.split('var1=')[1].replace('(','').replace(')',''))['result']['data']
    data = pd.DataFrame(data).rename(columns={'d':'datetime','o':'open','h':'high','l':'low','c':'close','v':'vol'})
    data = data.assign(datetime = data.datetime.apply(lambda x:pd.to_datetime(x)))
    data[['open','close','high','low','vol']] = data[['open','close','high','low','vol']].apply(pd.to_numeric)
    if data is None:
        return None
    else:
        return(data)

if __name__ == '__main__':
    pass