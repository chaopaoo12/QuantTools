import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_date_stamp
import itertools

def read_data_from_sina(options):
    driver = webdriver.Chrome(chrome_options=options)
    driver.get('http://vip.stock.finance.sina.com.cn/usstock/ustotal.php')
    soup = BeautifulSoup(driver.page_source, "html.parser").body
    driver.quit()
    a = [i.string.replace(')','').split('(') for i in list(itertools.chain.from_iterable([i for i in [i.find_all('a') for i in soup.find_all(class_='col_div')]]))]
    res = pd.DataFrame(a,columns=['name','code','N'])[['name','code']]
    return(res)

def get_usstock_list_sina():

    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Connection': 'keep-alive'
               }
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
    res = read_data_from_sina(options)
    if res is None:
        return None
    else:
        return(res)

if __name__ == '__main__':
    pass