# coding:utf-8

from fake_useragent import UserAgent
import requests
from bs4 import BeautifulSoup
import pymongo
import threading
import aiohttp
import asyncio
import time


# 存储
def insert_to_MongoDB(ip, score):
    if myCol.find_one({"IP": ip}) == None:  # 重复ip不存储
        myCol.insert_one({"IP": ip, "Score": score})


# 取出
def get_from_MongoDB(n=0):
    """
    :param n: 要返回的ip个数，默认为0取出全部
    :return:
    """
    r = myCol.find().sort("Score", -1).limit(n)
    return r


# 获取页面源码
def get_html(url):
    headers = {"User-Agent": ua.random}
    try:
        response = requests.get(url=url, headers=headers, timeout=5)
        if response.status_code == 200:
            return response.text
    except Exception:
        pass  # 获取源码失败
    # 如果不能访问，则使用ip池的代理ip进行尝试
    proxy_ips = get_from_MongoDB()
    for proxy_ip in proxy_ips:
        proxies = {"http": "http://" + proxy_ip["IP"], "https": "https://" + proxy_ip["IP"]}
        try:
            response_proxy = requests.get(url=url, headers=headers, proxies=proxies, timeout=5)
            if response_proxy.status_code == 200:
                return response_proxy.text
        except Exception:
            pass
    return ""  # 若所有代理均不能成功访问，则返回空字符串


# 西刺代理
def xicidaili():
    page = 3  # 要爬取的页数
    ip_list = []  # 临时存储爬取下来的ip
    for p in range(page + 1):
        url = "https://www.xicidaili.com/nn/" + str(p + 1)
        html = get_html(url)
        if html != "":
            soup = BeautifulSoup(html, 'lxml')
            ips = soup.find_all('tr', class_='odd')
            for i in ips:
                tmp = i.find_all('td')
                ip = tmp[1].text + ':' + tmp[2].text
                ip_list.append(ip)
                print('线程{}爬取ip:{}'.format(threading.current_thread().name, ip))
            time.sleep(3)
        else:
            print('西刺代理获取失败！')
            break
    for item in ip_list:
        queue_lock.acquire()
        insert_to_MongoDB(item, 10)
        queue_lock.release()


# 快代理
def kuaidaili():
    page = 10  # 要爬取的页数
    ip_list = []  # 临时存储爬取下来的ip
    for p in range(page + 1):
        url = "https://www.kuaidaili.com/free/inha/{}/".format(p + 1)
        html = get_html(url)
        if html != "":
            soup = BeautifulSoup(html, 'lxml')
            ips = soup.select('td[data-title="IP"]')
            ports = soup.select('td[data-title="PORT"]')
            for i in range(len(ips)):
                ip = ips[i].text + ':' + ports[i].text
                ip_list.append(ip)
                print('线程{}爬取ip:{}'.format(threading.current_thread().name, ip))
            time.sleep(3)
        else:
            print('快代理获取失败！')
            break
    for item in ip_list:
        queue_lock.acquire()
        insert_to_MongoDB(item, 10)
        queue_lock.release()


# 评分调整
def adjust_score(ip, myType):
    """
    验证成功的直接评分变为100，未验证成功的减1，评分为0的直接删除
    :param ip:
    :param type: 1 加分，-1 减分
    :return:
    """
    if myType == 1:
        query_ip = {"IP": ip}
        new_value = {"$set": {"Score": 100}}
        myCol.update_one(query_ip, new_value)
    elif myType == -1:
        query_ip = {"IP": ip}
        current_score = myCol.find_one(query_ip)["Score"]
        if current_score == 1:
            myCol.delete_one(query_ip)
        else:
            new_value = {"$set": {"Score": current_score - 1}}
            myCol.update_one(query_ip, new_value)


async def ip_test(url, headers, proxy):
    test_proxy = "http://" + proxy
    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        try:
            async with session.get(url=url, headers=headers, proxy=test_proxy) as resp:
                if resp.status == 200:
                    adjust_score(proxy, 1)
                else:
                    adjust_score(proxy, -1)
        except:
            adjust_score(proxy, -1)


# ip池测试
def pool_test():
    COUNTS = 100  # 每次测试100个ip
    ua = UserAgent()
    proxy_ips = list(get_from_MongoDB())
    test_url = "http://www.baidu.com"  # 可替换为要爬取的网址
    for i in range(0, len(proxy_ips), COUNTS):
        tasks = [ip_test(test_url, {"User-Agent": ua.random}, proxy["IP"]) for proxy in proxy_ips[i:i + COUNTS]]
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(tasks))
        print("共{}个，已测试{}个".format(len(proxy_ips) + 1, COUNTS + i))
        time.sleep(5)


# 爬取代理ip线程启动
def crawler_start(proxy_dict):
    global threads
    for proxy in proxy_dict.keys():
        thread = threading.Thread(target=proxy_dict[proxy], name=proxy)
        thread.start()
        threads.append(thread)
    for t in threads:  # 等待所有线程完成
        t.join()


if __name__ == '__main__':
    # 连接MongoDB数据库
    myClient = pymongo.MongoClient("mongodb://localhost:27017/")
    myDB = myClient["IPpool"]
    myCol = myDB["pool"]

    # 伪装用户代理
    ua = UserAgent()

    # 间隔5分钟爬取和测试一次
    while 1:
        # 爬取模块线程
        queue_lock = threading.Lock()
        threads = []
        proxy_dict = {"kuaidaili": kuaidaili, "xicidaili": xicidaili}
        # crawler_start(proxy_dict)

        # 测试模块线程
        print("代理ip爬取完毕，开始进行测试！")
        pool_test()
        print("测试完毕！")
        time.sleep(300)
