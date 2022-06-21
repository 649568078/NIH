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
            cursor.executemany(sql_insert, toinsert_tuple_list)
            conn.commit()
        except Exception as e:
            print(toinserts_values)
            print(e)
            print(sql_insert)
            conn.rollback()
            print("插入失败")


conn = pymysql.connect(host='localhost', user='root', password='a3x6v8f8', database='nih')
if conn:
    print('正常连接')

# 创建表
table_list = ["NIH_PI", "NIH_terms"]
cursor = conn.cursor()
for table in table_list:
    str = 'CREATE table {} (id INT primary key NOT NULL AUTO_INCREMENT)'.format(table)
    print(str)
    cursor.execute(str)
    conn.commit()

    if table == "NIH_PI":
        key_list = ['APPLICATION_ID', 'PI_NAME', 'PI_ID', 'FY', 'ADMINISTERING_IC', 'ORG_NAME']
        for key in key_list:
            str = 'alter table {} add {} Text'.format(table, key)
            cursor.execute(str)
            conn.commit()
    if table == "NIH_terms":
        key_list = ['APPLICATION_ID', 'TERM', 'FY', 'ADMINISTERING_IC','CORE_PROJECT_NUM','ACTIVITY']
        for key in key_list:
            str = 'alter table {} add {} Text'.format(table, key)
            cursor.execute(str)
            conn.commit()
cursor.close()

for i in [2016, 2017, 2018, 2019, 2020]:
    xml = open(r'C:\Users\Administrator\PycharmProjects\NIH分析\数据\X\project\RePORTER_PRJ_X_FY{}_new.xml'.format(i),
               encoding='utf-8').read()
    root = ET.fromstring(xml)
    print(root.tag)
    child = list(root)
    print(len(child))
    cursor = conn.cursor()
    for row in child:
        APPLICATION_ID = row.find('APPLICATION_ID').text
        FY = row.find('FY').text
        ADMINISTERING_IC = row.find('ADMINISTERING_IC').text
        ORG_NAME = row.find('ORG_NAME').text
        CORE_PROJECT_NUM = row.find('CORE_PROJECT_NUM').text
        ACTIVITY = row.find('ACTIVITY').text

        # 写入PI
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
            pilist.append((PI_NAME, PI_ID, APPLICATION_ID, FY, ADMINISTERING_IC, ORG_NAME))
            # insert_sql(conn, cursor, "NIH_PI", dict2)
        cursor.executemany(
            """ insert into NIH_PI(PI_NAME, PI_ID,APPLICATION_ID,FY,ADMINISTERING_IC,ORG_NAME) values(%s,%s,%s,%s,%s,%s)""",
            pilist)

        # 写入terms
        PROJECT_TERMS = row.find('PROJECT_TERMSX')
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
            termlist.append((TERM, APPLICATION_ID, FY, ADMINISTERING_IC,CORE_PROJECT_NUM,ACTIVITY))
            # insert_sql(conn, cursor, "NIH_terms", dict3)
        cursor.executemany(
            """ insert into NIH_terms(TERM, APPLICATION_ID,FY,ADMINISTERING_IC,CORE_PROJECT_NUM,ACTIVITY) values(%s,%s,%s,%s,%s,%s)""",
            termlist)
        conn.commit()
    cursor.close()
