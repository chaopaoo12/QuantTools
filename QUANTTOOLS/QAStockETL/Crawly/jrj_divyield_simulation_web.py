'''

'''
import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
import demjson
import time
from QUANTAXIS.QAUtil import (QA_util_getBetweenQuarter,QA_util_add_months,
                              QA_util_today_str,QA_util_datetime_to_strdate)

def read_stock_divyield(report_date, headers = None, page=1):
    if headers == None:
        headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                   'Accept-Language': 'zh-CN,zh;q=0.9',
                   'Cache-Control': 'max-age=0',
                   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
                   'Connection': 'keep-alive'
                   }
    args=  {"report_date": report_date, "unixstamp": int(round(time.time() * 1000))}

    strUrl1 = "http://stock.jrj.com.cn/report/js/sz/{report_date}.js?ts={unixstamp}".format(**args)
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
    driver = webdriver.Chrome(chrome_options=options)
    driver.maximize_window()
    driver.get(strUrl1)
    soup = BeautifulSoup(driver.page_source, "html.parser").body.text
    driver.quit()
    start_str = 'var fhps = '.format(**args)
    res = demjson.decode(soup.strip(start_str).strip(';').replace(''',
,
,''',',0,0,').replace(''',
,''',',0,'))
    data = pd.DataFrame(res['data'])
    if data.shape[0] > 0:
        page_num = res['summary']['total']
        data=data.drop_duplicates(keep='first')
        data.columns= ['a_stockcode','a_stocksname','div_info','div_type_code','bonus_shr',
                       'cash_bt','cap_shr','epsp','ps_cr','ps_up','reg_date','dir_dcl_date',
                       'a_stockcode1','ex_divi_date','prg']
        data = data.dropna(subset=['reg_date'])
        data['report_date']=report_date
        data['crawl_date']=QA_util_today_str()
        data['reg_date']=data['reg_date'].apply(lambda x:str(x).strip())
        data = data[data['reg_date'] != '']
        return(data, page_num)
    else:
        print("No divyield data for report date {report_date}. url: {url}".format(report_date = report_date,url=strUrl1))
        return(None,None)


def get_stock_divyield(report_date, headers = None, page=1):
    data, page_num = read_stock_divyield(report_date, headers, page)
    if data is None:
        data = pd.DataFrame()
    return(data)

if __name__ == '__main__':
    pass