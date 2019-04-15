import pandas as pd
from selenium import webdriver
from time import sleep
import os
from QUANTAXIS.QAUtil import QA_util_today_str,QA_util_date_stamp

def get_stock_report_ths(code):

    data = pd.DataFrame()

    for type in ['cash','benefit','debt']:
        excelFile = r'D:\{code}_{type}_report.xls'.format(code = code, type=type)
        seconds = 1
        headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                   'Accept-Language': 'zh-CN,zh;q=0.9',
                   'Cache-Control': 'max-age=0',
                   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
                   'Connection': 'keep-alive',
                   '''--proxy-server''': 'http://202.20.16.82:10152'
                   }

        while (os.path.exists(excelFile) != True):
            options = webdriver.ChromeOptions()
            for (key,value) in headers.items():
                options.add_argument('%s="%s"' % (key, value))
            prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': 'd:\\'}
            options.add_experimental_option('prefs', prefs)

            driver = webdriver.Chrome(chrome_options=options)
            driver.maximize_window()
            driver.get('http://basic.10jqka.com.cn/api/stock/export.php?export={type}&type=report&code={code}'.format(code = code, type=type))
            sleep(seconds)
            seconds = seconds + 1

        if os.path.exists(excelFile) == True:
            try:
                df1 = pd.DataFrame(pd.read_excel(excelFile, sheet_name='Worksheet')).T.reset_index()
                data = data.append(df1.T)
                driver.quit()

                try:
                    os.remove(excelFile)
                    print("Success Delete {code} {type} report file".format(code=code, type=type))
                except:
                    print("NO {code} {type} report file to Delete".format(code=code, type=type))
            except:
                print('Error for reading')

    res = data.T.iloc[1:,]
    new_index = data.T[0:1].values.tolist()[0]
    new_index[0] = "report_date"
    res.columns = new_index
    res["code"] = code
    res['crawl_date']=QA_util_today_str()
    return(res)