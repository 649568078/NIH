import requests
import json
import xlrd
import sys
import pickle
import time



def get_appid():
    workbook = xlrd.open_workbook(filename=r'C:\Users\Administrator\PycharmProjects\NIH分析\提取程序\RePORTER_PRJ_C_FY2020.xlsx')
    table = workbook.sheets()[0]
    table_list = table.col_values(0,start_rowx=1)
    fin_list = []
    for i in range(0, len(table_list), 400):
        list = table_list[i:i + 400]
        fin_list.append(list)
    with open('fin_list.pkl','wb') as f:
        pickle.dump(fin_list,f)


def download():
    with open('fin_list.pkl', 'rb') as f:
        fin_list = pickle.load(f)

    for i in range(0,len(fin_list)):
        idlist = fin_list.pop(0)
        print("剩余组："+str(len(fin_list)))

        url = "https://api.reporter.nih.gov/v1/projects/Search"
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
            "accept": "application/json",
            "Content-Type": "application/json",
        }

        payload = {
            "criteria":
                {
                    "appl_ids": idlist,
                },
            "limit": 500,
            "include_fields": [
                'ApplId', 'ActivityCode', 'AgencyIcAdmin', 'AwardType', 'award_notice_date',
                'BudgetStart', 'BudgetEnd',
                'CfdaCode', 'CoreProjectNum',
                'OrganizationType', 'ProjectNum', 'AgencyIcFundings', 'FundingMechanism',
                'FiscalYear', 'AgencyIcAdmin',
                'SpendingCategoriesDesc', 'Organization', 'CongDist',
                'PrincipalInvestigators', 'ProgramOfficers', 'ProjectStartDate',
                'ProjectEndDate', 'ProjectTitle', 'ProjectNumSplit',
                'FullStudySection', 'SubprojectId',
                'DirectCostAmt', 'IndirectCostAmt', 'AwardAmount',
                # 'PhrText','AbstractText', 'PrefTerms'
            ],
        }

        data = json.dumps(payload)
        res = requests.post(url, headers=headers, data=data)
        print(res)
        print(res.status_code)
        print(res.text)
        if res.status_code == 200:
            #保存结果
            with open("{}.json".format(len(fin_list)),'w') as f2:
                f2.write(res.text)
            #去除列表中的元素，写入到pickle
            with open('fin_list.pkl','wb') as f3:
                pickle.dump(fin_list,f3)

        time.sleep(2)

if __name__ == '__main__':
    #get_appid()

    download()