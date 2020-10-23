import re
from selenium import webdriver
from bs4 import BeautifulSoup
import datetime

def func1(str_t):
    if len(str_t) == 8:
        return(str_t)
    elif len(str_t) == 11:
        return(str_t.replace('年','').replace('月','').replace('日',''))
    elif len(str_t) == 10:
        if len(str_t.split('年')[1].split('月')[0]) < 2:
            return(str_t.split('年')[0]+'0'+str_t.split('年')[1].split('月')[0]+str_t.split('年')[1].split('月')[1]).replace('日','')
        else:
            return(str_t.split('年')[0]+str_t.split('年')[1].split('月')[0]+'0'+str_t.split('年')[1].split('月')[1]).replace('日','')
    elif len(str_t) == 9:
        return(str_t.replace('年','0').replace('月','0').replace('日',''))
    else:
        return(None)

def get_us_canlander(url = 'http://www.wstock.net/wstock/uholiday.htm'):
    driver = webdriver.Chrome()
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    can_text = [i.text for i in soup.find_all('li')]

    a_list  = [re.split('\t| |，|：',i)[0] for i in can_text[can_text.index('每年的1月1日，新年元旦，休市')+1:]]
    b_list  = [item for sublist in [i.split('、') for i in a_list if len(i) >3] for item in sublist]
    trans = str.maketrans("一二三四五六", "      ")
    c_list = [re.sub('[\W_]+', '',i).translate(trans).replace(' ','') for i in b_list]
    res = [func1(i) for i in c_list] + [func1(i) for i in [re.split('\t| |，',i)[1] for i in can_text[can_text.index('每年的1月1日，新年元旦，休市')+1:]] if len(i) <= 11 and len(i) >= 8 and i[0:3].isdigit()]
    return(res)

def getEveryDay(begin_date,end_date):
    date_list = []
    begin_date = datetime.datetime.strptime(begin_date, "%Y%m%d")
    end_date = datetime.datetime.strptime(end_date,"%Y%m%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y%m%d")
        date_list.append(date_str)
        begin_date += datetime.timedelta(days=1)
    return date_list

def get_usstock_canlander(start='20080101', end='20200930'):
    a_list = getEveryDay(start,end)
    b_list = get_us_canlander()
    return([i[0:4]+'-'+i[4:6]+'-'+i[6:8] for i in a_list if i not in b_list])