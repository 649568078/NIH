from xml.etree import ElementTree as ET
import xlsxwriter
import pymysql
import sys


def insert_sql(conn, cursor, tablename, toinserts_values):
    keys = ", ".join(toinserts_values.keys())
    qmark = ", ".join(["%s"] * len(toinserts_values))
    sql_insert = "insert into %s (%s) values (%s)" % (tablename, keys, qmark)
    try:
        cursor.execute(sql_insert, list(toinserts_values.values()))
        conn.commit()
        # print("插入成功")
    except Exception as e:
        print(e)
        print(sql_insert)
        conn.rollback()
        print("插入失败")


# 批量插入操作
def batch_insert_sql(tablename, toinsert_list):
    if len(toinsert_list) <= 0:
        pass
        #print('无值插入')
    else:
        toinsert_tuple_list = []
        for dictdd in toinsert_list:
            toinsert_tuple_list.append(tuple(dictdd.values()))
        toinserts_values = toinsert_list[0]
        keys = ", ".join(toinserts_values.keys())
        qmark = ", ".join(["%s"] * len(toinserts_values))
        sql_insert = "insert into %s (%s) values (%s)" % (tablename, keys, qmark)
        try:
            cursor.executemany(sql_insert, toinsert_tuple_list)
            conn.commit()
        except Exception as e:
            print(toinserts_values)
            print(e)
            print(sql_insert)
            conn.rollback()
            print("插入失败")


conn = pymysql.connect(host='localhost', user='root', password='xx19941130', database='nih')
if conn:
    print('正常连接')

# 创建表
table_list = ["NIH_总表", "NIH_PI", "NIH_terms"]
cursor = conn.cursor()
for table in table_list:
    str = 'CREATE table {} (id INT primary key NOT NULL AUTO_INCREMENT)'.format(table)
    print(str)
    cursor.execute(str)
    conn.commit()
    if table == "NIH_总表":
        key_list = ['APPLICATION_ID', 'ADMINISTERING_IC', 'BUDGET_START', 'BUDGET_END', 'FOA_NUMBER', 'FUNDING_ICs',
                    'FULL_PROJECT_NUM', 'CORE_PROJECT_NUM',
                    'CFDA_CODE', 'ORG_DUNS', 'ORG_NAME', 'SERIAL_NUMBER', 'TOTAL_COST', 'TOTAL_COST_SUB_PROJECT', 'FY',
                    'PROJECT_TITLE','ACTIVITY','SUPPORT_YEAR']
        for key in key_list:
            str = 'alter table {} add {} Text'.format(table, key)
            cursor.execute(str)
            conn.commit()
    if table == "NIH_PI":
        key_list = ['APPLICATION_ID', 'PI_NAME', 'PI_ID', 'FY', 'ADMINISTERING_IC', 'ORG_NAME', 'PROJECT_TITLE']
        for key in key_list:
            str = 'alter table {} add {} Text'.format(table, key)
            cursor.execute(str)
            conn.commit()
    if table == "NIH_terms":
        key_list = ['APPLICATION_ID', 'TERM', 'FY', 'ADMINISTERING_IC','CORE_PROJECT_NUM','ACTIVITY']
        for key in key_list:
            str = 'alter table {} add {} varchar(500)'.format(table, key)
            cursor.execute(str)
            conn.commit()
cursor.close()

for i in [2016, 2017, 2018, 2019, 2020]:
    xml = open('RePORTER_PRJ_X_FY{}_new.xml'.format(i), encoding='utf-8').read()
    root = ET.fromstring(xml)
    print(root.tag)
    child = list(root)
    print(len(child))
    for row in child:
        dict = {}
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

        # 写入总表
        cursor = conn.cursor()
        insert_sql(conn, cursor, "NIH_总表", dict)
        cursor.close()

        # 写入PI
        cursor = conn.cursor()
        PIS = row.find('PIS')
        pilist = []
        for pi in PIS:
            dict2 = {}
            PI_NAME = pi.find('PI_NAME').text
            dict2['PI_NAME'] = PI_NAME
            PI_ID = pi.find('PI_ID').text
            dict2['PI_ID'] = PI_ID
            dict2['APPLICATION_ID'] = APPLICATION_ID
            dict2['FY'] = FY
            dict2['ADMINISTERING_IC'] = ADMINISTERING_IC
            dict2['ORG_NAME'] = ORG_NAME
            dict2['PROJECT_TITLE'] = PROJECT_TITLE
            pilist.append(dict2)
            # insert_sql(conn, cursor, "NIH_PI", dict2)
        batch_insert_sql('NIH_PI',pilist)
        cursor.close()

        # 写入terms
        cursor = conn.cursor()
        PROJECT_TERMS = row.find('PROJECT_TERMSX')
        if PROJECT_TERMS is not None:
            termlist = []
            for term in PROJECT_TERMS:
                dict3 = {}
                TERM = term.text
                dict3['TERM'] = TERM
                dict3['APPLICATION_ID'] = APPLICATION_ID
                dict3['FY'] = FY
                dict3['ADMINISTERING_IC'] = ADMINISTERING_IC
                dict3['CORE_PROJECT_NUM'] = CORE_PROJECT_NUM
                dict3['ACTIVITY'] = ACTIVITY
                termlist.append(dict3)
                # insert_sql(conn, cursor, "NIH_terms", dict3)
            batch_insert_sql('NIH_terms', termlist)
        cursor.close()
