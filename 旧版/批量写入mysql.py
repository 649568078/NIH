import pymysql
from xml.etree import ElementTree as ET

# 批量插入操作
def batch_insert_sql(self, tablename, toinsert_list):
    if len(toinsert_list) <= 0:
        print('无值插入')
    else:
        toinsert_tuple_list = []
        for dictdd in toinsert_list:
            toinsert_tuple_list.append(tuple(dictdd.values()))
        toinserts_values = toinsert_list[0]
        keys = ", ".join(toinserts_values.keys())
        qmark = ", ".join(["%s"] * len(toinserts_values))
        sql_insert = "insert into %s (%s) values (%s)" % (tablename, keys, qmark)
        try:
            self.cursor.executemany(sql_insert, toinsert_tuple_list)
            self.conn.commit()
        except Exception as e:
            print(toinserts_values)
            print(e)
            print(sql_insert)
            self.conn.rollback()
            print("插入失败")


conn = pymysql.connect(host='localhost', user='root', password='xx19941130', database='nih')
if conn:
    print('正常连接')

xml = open('RePORTER_PRJ_X_FY2016_new.xml', encoding='utf-8').read()
root = ET.fromstring(xml)
print(root.tag)
child = list(root)
print(len(child))
dict = {}

for row in child:
    APPLICATION_ID = row.find('APPLICATION_ID').text
    dict['APPLICATION_ID'] = APPLICATION_ID
    ADMINISTERING_IC = row.find('ADMINISTERING_IC').text
    dict['ADMINISTERING_IC'] = ADMINISTERING_IC
    BUDGET_START = row.find('BUDGET_START').text
    dict['BUDGET_START'] = BUDGET_START
    BUDGET_END = row.find('BUDGET_END').text
    dict['BUDGET_END'] = BUDGET_END
    FOA_NUMBER = row.find('FOA_NUMBER').text
    dict['FOA_NUMBER'] = FOA_NUMBER
    FUNDING_ICs = row.find('FUNDING_ICs').text
    dict['FUNDING_ICs'] = FUNDING_ICs
    FULL_PROJECT_NUM = row.find('FULL_PROJECT_NUM').text
    dict['FULL_PROJECT_NUM'] = FULL_PROJECT_NUM
    CORE_PROJECT_NUM = row.find('CORE_PROJECT_NUM').text
    dict['CORE_PROJECT_NUM'] = CORE_PROJECT_NUM
    CFDA_CODE = row.find('CFDA_CODE').text
    dict['CFDA_CODE'] = CFDA_CODE
    ORG_DUNS = row.find('ORG_DUNS').text
    dict['ORG_DUNS'] = ORG_DUNS
    ORG_NAME = row.find('ORG_NAME').text
    dict['ORG_NAME'] = ORG_NAME
    SERIAL_NUMBER = row.find('SERIAL_NUMBER').text
    dict['SERIAL_NUMBER'] = SERIAL_NUMBER
    TOTAL_COST = row.find('TOTAL_COST').text
    dict['TOTAL_COST'] = TOTAL_COST
    TOTAL_COST_SUB_PROJECT = row.find('TOTAL_COST_SUB_PROJECT').text
    dict['TOTAL_COST_SUB_PROJECT'] = TOTAL_COST_SUB_PROJECT
    FY = row.find('FY').text
    dict['FY'] = FY
    PROJECT_TITLE = row.find('PROJECT_TITLE').text
    dict['PROJECT_TITLE'] = PROJECT_TITLE
    ACTIVITY = row.find('ACTIVITY').text
    dict['ACTIVITY'] = ACTIVITY
    SUPPORT_YEAR = row.find('SUPPORT_YEAR').text
    dict['SUPPORT_YEAR'] = SUPPORT_YEAR

    qmark = ", ".join(["%s"] * len(dict))

    print(qmark)