import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
from time import sleep
import os
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_date_stamp

def read_data_from_sina(code,report_year,report_type,table_name,options):
    driver = webdriver.Chrome(chrome_options=options)
    driver.get('http://money.finance.sina.com.cn/corp/go.php/vFD_{report_type}/stockid/{code}/ctrl/{report_year}/displaytype/4.phtml'.format(code=code,report_year=report_year,report_type=report_type))
    soup = BeautifulSoup(driver.page_source, "html.parser").body
    driver.quit()
    try:
        cols = soup.find_all(id=table_name)[0].find('tbody').find_all(class_='head')
    except:
        print('No {report_type} report For Stock {code} in year {year}'.format(report_type=report_type, code=code, year= report_year))
        return None
    values = list()
    for i in soup.find_all(id=table_name)[0].find('tbody').find_all('td'):
        values.append(i.text.strip())
    tar = ['三、筹资活动产生的现金流量','二、投资活动产生的现金流量','一、经营活动产生的现金流量','附注','六、每股收益',
           '资产','负债','所有者权益','流动资产','非流动资产','流动负债','非流动负债']
    values = [x for x in values if x not in tar]
    values = [0 if x == '--' else x.replace(',','') for x in values]
    cols = [x for x in cols if x not in tar]
    k = len(values)/len(cols)
    res1 = pd.DataFrame([values[i:i+int(k)] for i in range(0,len(values),int(k))]).T
    res = pd.concat([res1.iloc[1:,0],res1.iloc[1:,1:].astype(float)*10000],axis=1)
    res.columns = res1.iloc[0,]
    return(res.set_index('报表日期'))

def get_stock_report_sina(code,report_year):

    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Connection': 'keep-alive'
               }
    options = webdriver.ChromeOptions()
    for (key,value) in headers.items():
        options.add_argument('%s="%s"' % (key, value))
    res2 = pd.DataFrame()
    for years in report_year:
        res1 = pd.DataFrame()
        for report_type in ['BalanceSheet','ProfitStatement','CashFlow']:
            table_name = '{report_type}NewTable0'.format(report_type=report_type)
            if report_type == 'CashFlow':
                table_name = 'ProfitStatementNewTable0'
            res = read_data_from_sina(code,years,report_type,table_name,options)
            res1 = pd.concat([res1,res],axis=1)
        res2 = res2.append(res1, ignore_index=True)
    if res2 is None:
        return None
    else:
        res2['code'] = code
        return(res2)