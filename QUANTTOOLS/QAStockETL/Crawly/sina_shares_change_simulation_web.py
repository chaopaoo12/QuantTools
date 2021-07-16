import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
from time import sleep
import os
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_date_stamp

def read_data_from_sina(code,options):
    driver = webdriver.Chrome(chrome_options=options)
    driver.get('http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockStructure/stockid/{code}.phtml'.format(code=code))
    soup = BeautifulSoup(driver.page_source, "html.parser").find_all(class_='tagmain')[0].find_all(class_='table')
    driver.quit()
    import re
    strinfo = re.compile('·')
    table = pd.DataFrame()
    for i in soup:
        try:
            cols = i.find_all('tbody')[0].find_all(class_='head')
            cols = ['begin_date' if x == '变动日期' else strinfo.sub('',x.text.strip()).replace('''(历史记录)''','') for x in cols]
            cols = [x for x in cols if x not in ['股本结构图','点击查看明细']]
            cols = ['begin_date' if x == '变动日期' else x for x in cols]
            cols = ['send_date' if x == '公告日期' else x for x in cols]
            cols = ['reason' if x == '变动原因' else x for x in cols]
            cols = ['total_shares' if x == '总股本' else x for x in cols]
            cols = ['tra_ashares' if x == '流通A股' else x for x in cols]
            cols = ['nontra_ashares' if x == '限售A股' else x for x in cols]
            cols = ['tra_bshares' if x == '流通B股' else x for x in cols]
            cols = ['nontra_bshares' if x == '限售B股' else x for x in cols]
            cols = ['tra_hshares' if x == '流通H股' else x for x in cols]
            cols = ['pre_shares' if x == '优先股' else x for x in cols]
            cols = ['exe_shares' if x == '高管股' else x for x in cols]

        except:
            print('Not Found Stock {code} '.format(code=code))

        values = list()
        for i in i.find_all('tbody')[0].find_all('td'):
            if i.text.strip() not in ['·股本结构图','点击查看明细']:
                values.append(strinfo.sub('',i.text.strip()))
        values = [0 if x == '--' else x.replace('万股','') for x in values]
        k = len(values)/(len(cols)-1)
        res1 = pd.DataFrame([values[i:i+int(k)] for i in range(0,len(values),int(k))]).T
        table = table.append(res1.iloc[1:,])
    table.columns = cols
    for i in [x for x in cols if x not in ['reason','begin_date','send_date','流通股']]:
        table[i] = table[i].astype(float)
    table['code'] = code
    return(table)

def get_stock_shares_sina(code):

    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Connection': 'keep-alive'
               }
    options = webdriver.ChromeOptions()
    for (key,value) in headers.items():
        options.add_argument('%s="%s"' % (key, value))
    options.add_argument('headless')
    res = read_data_from_sina(code,options)
    res['crawl_date']=QA_util_today_str()
    if res is None:
        return None
    else:
        return(res)

if __name__ == '__main__':
    pass