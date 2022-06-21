import os
import json

#检查文件的完整性
def check():
    #path = r'D:\NIH基金\提取程序\2016-2019数据\ABS'
    path = r'D:\NIH基金\提取程序\2020年API数据\2020年除大字段外的全结果'
    json_list = os.listdir(path)
    wrong_list = []
    appid_list = []
    for i in json_list:
        js_data = path + '\\{}'.format(i)
        # 打开json文档
        with open(js_data,'r',encoding='utf-8') as f:
            try:
                data = json.load(f)
                results = data['results']
                total = data['meta']['total']
                if total != 100:
                    print(i+ str(total))
                # 提取json文件中的字典写入dict_list
                for i in results:
                    APPLICATION_ID = i['appl_id']
                    appid_list.append(APPLICATION_ID)
            except Exception as e:
                print(i)
                print(e)
                wrong_list.append(i)
    print(wrong_list)
    print(len(wrong_list))
    print(len(set(appid_list)))

#检查文件的连续性
def check_number():
    jieguo = os.listdir('结果')
    #print(jieguo)
    new_list = []
    for i in jieguo:
        a = i.replace(".json","")
        new_list.append(a)
    print(len(new_list))
    print(new_list)
    balance_list = []
    for a in range(1, 103765):
        if a not in new_list:
            balance_list.append(a)
    print(balance_list)

check()