import pandas as pd
from selenium import webdriver
from time import sleep
import os
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_date_stamp
from bs4 import BeautifulSoup

def get_stock_report_wy(code):

    data = pd.DataFrame()

    for type in ['lrb','zcfzb','xjllb']:
        excelFile = r'D:\{type}{code}.csv'.format(code = code, type=type)
        seconds = 1
        headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                   'Request URL': 'http://quotes.money.163.com/service/{type}_{code}.html'.format(code=code,type=type),
                   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
                   'Remote Address':'59.111.160.246:80',
                   'Referrer Policy':'no-referrer-when-downgrade'
                   }

        while (os.path.exists(excelFile) != True):
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
            prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': 'd:\\'}
            options.add_experimental_option('prefs', prefs)
            driver = webdriver.Chrome(chrome_options=options)
            driver.get('http://quotes.money.163.com/service/{type}_{code}.html'.format(code = code, type=type))
            sleep(seconds)
            seconds = seconds + 1

        if os.path.exists(excelFile) == True:
            try:
                df1 = pd.read_csv(excelFile,encoding='ANSI', na_values=["--"," --"," "],header=0).T
                res = df1.reset_index().iloc[1:,:]
                res.columns= [x.replace('(万元)','').replace(' ','').strip() for x in df1.reset_index().iloc[:1].values.tolist()[0]]
                if type == 'xjllb':
                    res.columns= [x+'C' if x in ['财务费用', '净利润', '少数股东损益'] else x.replace('(万元)','').replace(' ','').strip() for x in list(res.columns)]
                res = res.set_index('报告日期')
                data = pd.concat([data,res],axis=1,sort=False).fillna(0)
                driver.quit()
                try:
                    os.remove(excelFile)
                    print("Success Delete {code} {type} report file".format(code=code, type=type))
                except:
                    print("NO {code} {type} report file to Delete".format(code=code, type=type))
            except:
                print('Error for reading')
    data = data * 10000
    res = data.reset_index()
    new_index = list(res.columns)
    new_index[0] = "report_date"
    res.columns = new_index
    res["code"] = code
    res['crawl_date']=QA_util_today_str()
    res = res[res['report_date'].str.contains('Unnamed')==0]
    res = res[res['report_date'].apply(len) == 10]
    return(res)

def read_data_data_from_wy(code,report_type):
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

    driver = webdriver.Chrome(options = options)
    driver.get('http://quotes.money.163.com/f10/{report_type}_{code}.html'.format(report_type=report_type,code=code))
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    index = [i.string.replace('(万元)','') for i in soup.find_all(class_='col_l')[0].tbody.find_all(class_='td_1')]
    if report_type == 'lrb':
        index = ['少数股东损益' if x == '少数股东损益' else x for x in index]
    elif report_type == 'xjllb':
        index = ['少数股东损益C' if x == '少数股东损益' else x for x in index]
        index = ['净利润C' if x == '净利润' else x for x in index]
        index = ['财务费用C' if x == '财务费用' else x for x in index]
    cols = [i.string for i in soup.find_all(class_='col_r')[0].find_all(class_='dbrow')[0].find_all('th')]
    x = []
    for i in soup.find_all(class_='col_r')[0].find_all('tr'):
        x.append([float(i.string.replace(',','').replace('--','0')) * 10000 for i in i.find_all('td') if i.string is not None])
    res =pd.DataFrame(x[1:],index=index,columns=cols).T
    return(res)

def read_stock_report_wy(code):
    res1 = pd.DataFrame()
    for report_type in ['zcfzb','lrb','xjllb']:
        res = read_data_data_from_wy(code,report_type)
        if res1.shape[0]==0:
            res1 = res
        else:
            res1 = res1.join(res)
    if res1 is None:
        return None
    else:
        res1['code'] = code
        res1['crawl_date']=QA_util_today_str()
        res = res1.reset_index()
        try:
            res.columns = ['report_date' if x == 'index' else x for x in list(res1.reset_index().columns)]
        except:
            pass
        return(res)

if __name__ == '__main__':
    pass