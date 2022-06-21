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



conn = pymysql.connect(host='localhost', user='root', password='xx19941130', database='nih')
if conn:
    print('正常连接')

# 创建表
table_list = ["NIH_CAT2016_2019"]
cursor = conn.cursor()
for table in table_list:
    str = 'CREATE table {} (id INT primary key NOT NULL AUTO_INCREMENT)'.format(table)
    print(str)
    cursor.execute(str)
    conn.commit()

    if table == "NIH_CAT2016_2019":
        key_list = ['APPLICATION_ID', 'NIH_SPENDING_CATS', 'FY', 'ADMINISTERING_IC']
        for key in key_list:
            str = 'alter table {} add {} Text'.format(table, key)
            cursor.execute(str)
            conn.commit()
cursor.close()

for i in [2016, 2017, 2018, 2019, 2020]:
    xml = open(r'D:\NIH基金\数据\X\project\RePORTER_PRJ_X_FY{}_new.xml'.format(i),
               encoding='utf-8').read()
    root = ET.fromstring(xml)
    print(root.tag)
    child = list(root)
    print(len(child))

    cursor = conn.cursor()
    for row in child:
        # 单层数据
        APPLICATION_ID = row.find('APPLICATION_ID').text
        FY = row.find('FY').text
        ADMINISTERING_IC = row.find('ADMINISTERING_IC').text
        # 写入Cat
        NIH_SPENDING_CATS = row.find('NIH_SPENDING_CATS')
        print(NIH_SPENDING_CATS)

        cat_list = []
        for Cat in NIH_SPENDING_CATS:
            dict3 = {}
            Cat = Cat.text
            dict3['APPLICATION_ID'] = APPLICATION_ID
            dict3['Cat'] = Cat
            dict3['FY'] = FY
            dict3['ADMINISTERING_IC'] = ADMINISTERING_IC
            cat_list.append((APPLICATION_ID, Cat, FY, ADMINISTERING_IC))
        print(cat_list)
        cursor.executemany(""" insert into NIH_CAT2016_2019(APPLICATION_ID,NIH_SPENDING_CATS,FY,ADMINISTERING_IC) values(%s,%s,%s,%s)""",
                           cat_list)
        conn.commit()
    cursor.close()
