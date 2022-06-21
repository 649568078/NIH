import pandas as pd
import re
import xlrd

def get_funding():
    data = pd.read_excel("2021NLM.xls")
    # frame = pd.DataFrame(data,columns=['APPLICATION_ID','FUNDING_ICs'])
    # print(frame)
    funding_list = []
    for i in data['FUNDING_ICs']:
        print(i)
        print(type(i))
        if i == i:#排除NAN
            each_funding = i.split('\\')
            for a in each_funding:
                if a != '':
                    print(a)
                    ic = re.search('(.*?):.*',a).group(1)
                    funding = re.search('.*?:(.*)',a).group(1)
                    funding_list.append((ic,funding))
                    print(ic,funding)
    #print(funding_list)
    total = {}
    total_key = total.keys()
    for i in funding_list:
        if i[0] not in total_key:
            total[i[0]] = int(i[1])
        elif i[0] in total_key:
            total[i[0]] += int(i[1])
        print(total[i[0]])
    print(total)
   # print(data.columns)
    a = 0
    for ff in total.values():
        a += int(ff)
    print(a)


get_funding()