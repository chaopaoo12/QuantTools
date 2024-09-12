'''

'''
import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
import demjson
from QUANTAXIS.QAUtil import (QA_util_getBetweenQuarter,QA_util_add_months,
                              QA_util_today_str,QA_util_datetime_to_strdate)

def read_financial_report_date(report_date, headers = None, psize= 2000,vname="plsj",page=1):
    if headers == None:
        headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                   'Accept-Language': 'zh-CN,zh;q=0.9',
                   'Cache-Control': 'max-age=0',
                   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
                   'Connection': 'keep-alive'
                   }
    args=  {"report_date": report_date, "psize": psize, "vname": vname, 'page': page}

    strUrl1 = "http://app.jrj.com.cn/jds/data_ylj.php?cid=1002&_pd=&_pd2=&_pid={report_date}&ob=2&od=d&page={page}&psize={psize}&vname={vname}".format(**args)
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
    start_str = 'var {vname} = '.format(**args)
    res = demjson.decode(soup.strip(start_str).strip(';'))
    pages = args['page']
    data = pd.DataFrame(res['data'])
    page_num = res['summary']['pages']
    while pages < page_num:
        pages = pages + 1
        res, page_num = read_financial_report_date(report_date,headers,page = pages)
        data = data.append(res)
    data=data.drop_duplicates(keep='first')
    return(data, page_num)

def get_financial_report_date(report_date, headers = None, psize= 2000,vname="plsj",page=1):
    data, page_num = read_financial_report_date(report_date, headers, psize,vname,page)
    data.columns= ['code','name','pre_date','first_date','second_date','third_date','real_date','codes']
    data = data.dropna(subset=["real_date"])
    data['report_date']=report_date
    data['crawl_date']=QA_util_today_str()
    return(data[data["real_date"].apply(lambda x: len(x)!=0)])

if __name__ == '__main__':
    pass