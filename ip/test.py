# coding:utf-8

from fake_useragent import UserAgent
from redis import StrictRedis
import aiohttp
import asyncio
import time


# 取出
def get_from_Redis():
    """
    :param n: 要返回的ip个数，默认为0取出全部
    :return:
    """
    r = store.zrangebyscore('ip_pool', 1, 9)
    return r


# 稳定ip测试
def get_from_steady_ip():
    """
    :param n: 要返回的ip个数，默认为0取出全部
    :return:
    """
    r = store.zrangebyscore('steady_ip', 1, 99)
    return r


# 稳定ip池测试
def pool_steady_test():
    # 每次测试100个ip
    ua = UserAgent()
    proxy_ips = list(get_from_steady_ip())
    test_url = "http://www.baidu.com"  # 可替换为要爬取的网址
    for i in range(0, len(proxy_ips)):
        tasks = [ip_test(test_url, {"User-Agent": ua.random}, str(proxy_ips[i]).replace('\'', '').replace('b', ''))]
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(tasks))
        print("共{}个，已测试{}个".format(len(proxy_ips) + 1, i))
        time.sleep(1)


# ip池测试
def pool_test():
    # 每次测试100个ip
    COUNTS = 100
    ua = UserAgent()
    proxy_ips = list(get_from_Redis())
    test_url = "http://www.baidu.com"  # 可替换为要爬取的网址
    for i in range(0, len(proxy_ips), COUNTS):
        # for i in range(0, len(proxy_ips)):
        tasks = [ip_test(test_url, {"User-Agent": ua.random}, str(proxy).replace('\'', '').replace('b', ''))
                 for proxy in proxy_ips[i:i + COUNTS]]
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(tasks))
        print("共{}个，已测试{}个".format(len(proxy_ips) + 1, COUNTS + i))
        # print("共{}个，已测试{}个".format(len(proxy_ips) + 1, i))
        time.sleep(5)


# 评分调整
def adjust_score(ip, myType):
    """
    验证成功的直接评分变为100，未验证成功的减1，评分为0的直接删除
    :param ip:
    :param type: 1 加分，-1 减分
    :return:
    """
    if myType == 1:
        store.zincrby('steady_ip', 10, ip)
    elif myType == -1:
        current_score = store.zscore('ip_pool', ip)
        if current_score == 1:
            store.zrem("ip_pool", ip)
        else:
            store.zincrby('ip_pool', -1, ip)


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


if __name__ == '__main__':
    # 连接redis数据库
    store = StrictRedis(host='localhost', port=6379, db=0)

    # 伪装用户代理
    ua = UserAgent()
    # 测试模块线程
    # while 1:
    print("代理ip爬取完毕，开始进行测试！")
    # pool_test()
    pool_steady_test()
    print("测试完毕！")
    # time.sleep(600)
