import pandas as pd
from selenium import webdriver
from time import sleep
import os
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_date_stamp

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
    res = res * 10000
    res = data.reset_index()
    new_index = list(res.columns)
    new_index[0] = "report_date"
    res.columns = new_index
    res["code"] = code
    res['crawl_date']=QA_util_today_str()
    res = res[res['report_date'].str.contains('Unnamed')==0]
    res = res[res['report_date'].apply(len) == 10]
    return(res)