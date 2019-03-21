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

        while (os.path.exists(excelFile) != True):
            print(seconds)
            options = webdriver.ChromeOptions()
            prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': 'd:\\'}
            options.add_experimental_option('prefs', prefs)

            driver = webdriver.Chrome(chrome_options=options)
            driver.get('http://basic.10jqka.com.cn/api/stock/export.php?export={type}&type=report&code={code}'.format(code = code, type=type))
            sleep(seconds)
            driver.quit()
            seconds = seconds + 1

        df1 = pd.DataFrame(pd.read_excel(excelFile, sheet_name='Worksheet')).T.reset_index()
        data = data.append(df1.T)

        try:
            os.remove(excelFile)
            print("Success Delete {code} {type} report file".format(code=code, type=type))
        except:
            print("NO {code} {type} report file to Delete".format(code=code, type=type))

    res = data.T.iloc[1:,]
    new_index = data.T[0:1].values.tolist()[0]
    new_index[0] = "report_date"
    res.columns = new_index
    res["code"] = code
    res['crawl_date']=QA_util_today_str()
    return(res)