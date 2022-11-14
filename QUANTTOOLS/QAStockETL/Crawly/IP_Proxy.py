import requests
from bs4 import BeautifulSoup

def get_ip_poll(url='http://www.ip3366.net/free/?stype=1&page=1'):
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Connection': 'keep-alive'
               }
    response = requests.get(url=url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser").body
    port = [i.find_all('td')[1].text for i in soup.find_all('tr')[1:]]
    ip = [i.find_all('td')[0].text for i in soup.find_all('tr')[1:]]
    proxy = []
    for i in range(len(port)):
        proxy.append(ip[i] + ':' + port[i])
    return(proxy)


def check_ip_poll(proxy):
    #useful_proxy = []
    proxies = {
        "http": "http://" + proxy,
    }
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Language': 'zh-CN,zh;q=0.9',
               'Cache-Control': 'max-age=0',
               'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
               'Connection': 'keep-alive'
               }
    try:
        response = requests.get(url='https://www.baidu.com/',headers=headers,proxies=proxies,timeout=1) #设置timeout，使响应等待1s
        response.close()
        if response.status_code == 200:
            #useful_proxy.append(proxy)
            print(proxy, '\033[31m可用\033[0m')
            return(proxy)
        else:
            print(proxy, '不可用')
            return(None)
    except:
        print(proxy,'请求异常')
        return(None)

