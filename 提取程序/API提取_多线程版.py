import requests
import json
import xlrd
import sys
import pickle
import time
import csv
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor
import re


def get_proxies():
    # 提取代理API接口，获取代理IP
    api_url = "http://webapi.http.zhimacangku.com/getip?num=20&type=2&pro=&city=0&yys=0&port=11&pack=231713&ts=0&ys=0&cs=0&lb=1&sb=0&pb=5&mr=1&regions="

    proxy_ips = requests.get(api_url).text.replace('false', 'False').replace('true', 'True').replace('null', '""')
    print(proxy_ips)
    proxy_ips = eval(proxy_ips)['data']
    proxies_list = []
    for proxy_ip in proxy_ips:
        ip = proxy_ip["ip"]
        port = proxy_ip["port"]
        proxyMeta = "http://%(host)s:%(port)s" % {
            "host": ip,
            "port": port,
        }
        pro = {'http': proxyMeta, 'https': proxyMeta}
        proxies_list.append(pro)
    f = open("proxies.pkl", 'wb')
    pickle.dump(proxies_list, f)
    return proxies_list


def get_one_proxies(t_sequence):
    # 提取代理API接口，获取代理IP
    api_url = "http://webapi.http.zhimacangku.com/getip?num=1&type=2&pro=&city=0&yys=0&port=11&pack=231713&ts=0&ys=0&cs=0&lb=1&sb=0&pb=5&mr=1&regions="

    proxy_ips = requests.get(api_url).text.replace('false', 'False').replace('true', 'True').replace('null', '""')
    print("替换代理线程：{}".format(t_sequence)+str(proxy_ips))
    proxy_ips = eval(proxy_ips)['data']
    for proxy_ip in proxy_ips:
        ip = proxy_ip["ip"]
        port = proxy_ip["port"]
        proxyMeta = "http://%(host)s:%(port)s" % {
            "host": ip,
            "port": port,
        }
        pro = {'http': proxyMeta, 'https': proxyMeta}
        proxies[t_sequence] = pro
    f = open("proxies.pkl", 'wb')
    pickle.dump(proxies, f)


def get_project_nums():
    with open(r'D:\NIH基金\提取程序\数据\2016-2019Cat查询条件.csv') as f:
        f_csv = csv.reader(f)
        col = [row[0] for row in f_csv]
        col.pop(0)
        print(col)
        print(len(col))
        fin_list = []
        count = 1
        for i in range(0, len(col), 100):
            list = col[i:i + 100]
            fin_list.append((count, list))
            count += 1
        with open('fin_list.pkl', 'wb') as f:
            pickle.dump(fin_list, f)
        with open('fin_list_bak.pkl', 'wb') as f:
            pickle.dump(fin_list, f)
    print(fin_list)
    print(len(fin_list))


def re_download():
    re_downloadlist = ['1023.json']
    re_list = []
    with open('fin_list_bak.pkl', 'rb') as f:
        fin_list = pickle.load(f)
        print(len(fin_list))
    for i in re_downloadlist:
        num = i.split('.json')[0]
        print(num)
        for a in fin_list:
            if str(a[0]) == str(num):
                re_list.append(a)
    print(re_list)
    print(len(re_list))
    with open('fin_list.pkl', 'wb') as f:
        pickle.dump(re_list, f)


def download(tumple):
    time.sleep(10)
    # 打印线程名字
    t = threading.currentThread()
    print(t.getName())
    print('tumple' + str(tumple))
    # 线程编号
    t_sequence = re.search('ThreadPoolExecutor-0_(.)', t.getName()).group(1)
    lock = threading.Lock()
    # 获取代理地址
    proxies_copy = proxies.copy()
    proxie = proxies_copy[int(t_sequence)]
    print(str(proxie))
    try:
        # 获取数据
        id = tumple[0]
        Appid_list = tumple[1]
        # print(str(id), str(Appid_list))
        url = "https://api.reporter.nih.gov/v1/projects/Search"
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
            "accept": "application/json",
            "Content-Type": "application/json",
        }
        payload = {
            "criteria":
                {
                    "appl_ids": Appid_list,
                },
            "limit": 500,
            "include_fields": [
                # 'ApplId', 'ActivityCode', 'AgencyIcAdmin', 'AwardType', 'award_notice_date',
                # 'BudgetStart', 'BudgetEnd',
                # 'CfdaCode', 'CoreProjectNum','Organization','OrganizationType',
                # 'ProjectNum', 'AgencyIcFundings', 'FundingMechanism',
                # 'FiscalYear', 'AgencyIcAdmin',
                # 'CongDist','PrincipalInvestigators', 'ProgramOfficers', 'ProjectStartDate',
                # 'ProjectEndDate', 'ProjectNumSplit','FullStudySection', 'SubprojectId',
                # 'DirectCostAmt', 'IndirectCostAmt', 'AwardAmount',
                'ApplId', 'PhrText', 'AbstractText', 'ProjectTitle','PrefTerms','SpendingCategoriesDesc'
            ],
        }

        data = json.dumps(payload)
        # print(data)
        res = requests.post(url, headers=headers, data=data, timeout=40, proxies=proxie)
        # res = requests.post(url, headers=headers, data=data, timeout=40)
        print(res)
        print(res.status_code)
        print(res.text)
        if res.status_code == 200:
            # 保存结果
            with lock:
                with open(r"D:\NIH基金\提取程序\2016-2019数据\ABS\{}.json".format(id), 'w', encoding='utf-8') as f2:
                    f2.write(res.text)
                    print("{}.json".format(str(id) + '下载完成'))
                # 删除
                # print("剩余组：" + str(len(fin_list)))
                fin_list.remove(tumple)
                # print("再剩余组：" + str(len(fin_list)))
                # 去除列表中的元素，写入到pickle
                with open('fin_list.pkl', 'wb') as f3:
                    pickle.dump(fin_list, f3)
    except Exception:
        # print(traceback.format_exc())
        get_one_proxies(int(t_sequence))


if __name__ == '__main__':
    # get_project_nums()
    # get_proxies()
    re_download()
    #
    proxies = pickle.load(open('proxies.pkl', 'rb'))
    print('获取到的IP数量:' + str(len(proxies)))
    print(proxies)

    with open('fin_list.pkl', 'rb') as f:
        # global fin_list
        fin_list = pickle.load(f)
        print(len(fin_list))

    max_workers = 20
    pool = ThreadPoolExecutor(max_workers=max_workers)
    pool.map(download, fin_list)
