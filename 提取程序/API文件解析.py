import os
import json
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

#Cat插入
def cat_insert():
    # 创建Cat表
    table_list = ["nih_cat_2020"]
    cursor = conn.cursor()
    for table in table_list:
        str = 'CREATE table {} (id INT primary key NOT NULL AUTO_INCREMENT)'.format(table)
        print(str)
        cursor.execute(str)
        conn.commit()

        if table == "nih_cat_2020":
            key_list = ['APPLICATION_ID', 'NIH_SPENDING_CATS']
            for key in key_list:
                str = 'alter table {} add {} Text'.format(table, key)
                cursor.execute(str)
                conn.commit()
        cursor.close()

    path = r'D:\NIH基金\提取程序\2020年API数据\2020年除大字段外的全结果'
    json_list = os.listdir(path)
    for i in json_list:
        js_data = path + '\\{}'.format(i)
        # 打开json文档
        with open(js_data,'r',encoding='utf-8') as f:
            data = json.load(f)
            #print(data)
            #print(type(data))
            results = data['results']
            dict_list = []
            #提取json文件中的字典写入dict_list
            for i in results:
                dict = {}
                APPLICATION_ID = i['appl_id']
                dict['APPLICATION_ID'] = APPLICATION_ID

                NIH_SPENDING_CATS = i['spending_categories_desc']
                if NIH_SPENDING_CATS is not None:
                    cat_list = NIH_SPENDING_CATS.split(";")
                    for cat in cat_list:
                        dict['NIH_SPENDING_CATS'] = cat.strip(' ')
                        dict_list.append(dict.copy())
                        print(dict)

            #sys.exit()
        batch_insert_sql("nih_cat_2020", dict_list)
#Terms插入
def terms_insert():
    # 创建Cat表
    table_list = ["NIH_terms_2020"]
    cursor = conn.cursor()
    for table in table_list:
        str = 'CREATE table {} (id INT primary key NOT NULL AUTO_INCREMENT)'.format(table)
        print(str)
        cursor.execute(str)
        conn.commit()

        if table == "NIH_terms_2020":
            key_list = ['APPLICATION_ID', 'terms']
            for key in key_list:
                str = 'alter table {} add {} Text'.format(table, key)
                cursor.execute(str)
                conn.commit()
        cursor.close()

    path = r'D:\NIH基金\提取程序\2020年API数据\2020年大字段数据'
    json_list = os.listdir(path)
    for i in json_list:
        js_data = path + '\\{}'.format(i)
        # 打开json文档
        with open(js_data,'r',encoding='utf-8') as f:
            data = json.load(f)
            #print(data)
            #print(type(data))
            results = data['results']
            dict_list = []
            #提取json文件中的字典写入dict_list
            for i in results:
                dict = {}
                APPLICATION_ID = i['appl_id']
                dict['APPLICATION_ID'] = APPLICATION_ID
                pref_terms = i['pref_terms']
                if pref_terms is not None:
                    terms_list = pref_terms.split(";")
                    for terms in terms_list:
                        dict['terms'] = terms.strip(' ')
                        dict_list.append(dict.copy())
                        print(dict)

            #sys.exit()
        batch_insert_sql("NIH_terms_2020", dict_list)
#脑神经2020数据插入
def brain_2020_insert():
    # 创建Cat表
    table_list = ["脑神经nih_abs_2020"]
    cursor = conn.cursor()
    for table in table_list:
        str = 'CREATE table {} (id INT primary key NOT NULL AUTO_INCREMENT)'.format(table)
        print(str)
        cursor.execute(str)
        conn.commit()

        if table == "脑神经nih_abs_2020":
            key_list = ['APPLICATION_ID','abstract_text','phr_text']
            for key in key_list:
                str = 'alter table {} add {} Text'.format(table, key)
                cursor.execute(str)
                conn.commit()
        cursor.close()

    path = r'D:\NIH基金\提取程序\2020年API数据\2020年大字段数据'
    json_list = os.listdir(path)
    for i in json_list:
        js_data = path + '\\{}'.format(i)
        # 打开json文档
        with open(js_data,'r',encoding='utf-8') as f:
            data = json.load(f)
            #print(data)
            #print(type(data))
            results = data['results']
            dict_list = []
            #提取json文件中的字典写入dict_list
            for i in results:
                dict = {}
                APPLICATION_ID = i['appl_id']
                dict['APPLICATION_ID'] = APPLICATION_ID
                abstract_text = i['abstract_text']
                dict['abstract_text'] = abstract_text
                phr_text = i['phr_text']
                dict['phr_text'] = phr_text
                dict_list.append(dict.copy())

            #sys.exit()
        batch_insert_sql("脑神经nih_abs_2020", dict_list)
#脑神经2016-2019数据插入
def brain2016_2019_insert():
    # 创建Cat表
    table_list = ["脑神经NIH_ABS_2016_2019"]
    cursor = conn.cursor()
    for table in table_list:
        str = 'CREATE table {} (id INT primary key NOT NULL AUTO_INCREMENT)'.format(table)
        print(str)
        cursor.execute(str)
        conn.commit()

        if table == "脑神经NIH_ABS_2016_2019":
            key_list = ['APPLICATION_ID','abstract_text','project_title','phr_text']
            for key in key_list:
                str = 'alter table {} add {} Text'.format(table, key)
                cursor.execute(str)
                conn.commit()
        cursor.close()

    path = r'D:\NIH基金\提取程序\2016-2019数据\ABS'
    json_list = os.listdir(path)
    for i in json_list:
        js_data = path + '\\{}'.format(i)
        # 打开json文档
        with open(js_data,'r',encoding='utf-8') as f:
            data = json.load(f)
            #print(data)
            #print(type(data))
            results = data['results']
            dict_list = []
            #提取json文件中的字典写入dict_list
            for i in results:
                dict = {}
                APPLICATION_ID = i['appl_id']
                dict['APPLICATION_ID'] = APPLICATION_ID
                abstract_text = i['abstract_text']
                dict['abstract_text'] = abstract_text
                project_title = i['project_title']
                dict['project_title'] = project_title
                phr_text = i['phr_text']
                dict['phr_text'] = phr_text
                dict_list.append(dict.copy())
                #print(dict)

            #sys.exit()
        batch_insert_sql("脑神经NIH_ABS_2016_2019", dict_list)
#NIH2020年总表update插入
def add_insert():
    path = r'D:\NIH基金\提取程序\2020年API数据\2020年除大字段外的全结果'
    json_list = os.listdir(path)
    for i in json_list:
        js_data = path + '\\{}'.format(i)
        # 打开json文档
        with open(js_data,'r',encoding='utf-8') as f:
            data = json.load(f)
            #print(data)
            #print(type(data))
            results = data['results']
            #提取json文件中的字典写入dict_list
            for i in results:
                APPLICATION_ID = i['appl_id']
                project_title = i['project_title']
                str = 'UPDATE nih_总表_2020 SET project_title = "{}" WHERE APPLICATION_ID ="{}"'.format(project_title,APPLICATION_ID)
                try:
                    cursor.execute(str)
                    conn.commit()
                except Exception as e:
                    print(e)
                    print("插入失败")
#NIH2020年title单独导出
def cat_2020_title_insert():
    # 创建Cat表
    table_list = ["nih_title_2020"]
    cursor = conn.cursor()
    for table in table_list:
        str = 'CREATE table {} (id INT primary key NOT NULL AUTO_INCREMENT)'.format(table)
        print(str)
        cursor.execute(str)
        conn.commit()

        if table == "nih_title_2020":
            key_list = ['APPLICATION_ID','project_title']
            for key in key_list:
                str = 'alter table {} add {} Text'.format(table, key)
                cursor.execute(str)
                conn.commit()
        cursor.close()

    path = r'D:\NIH基金\提取程序\2020年API数据\2020年除大字段外的全结果'
    json_list = os.listdir(path)
    for i in json_list:
        js_data = path + '\\{}'.format(i)
        # 打开json文档
        with open(js_data,'r',encoding='utf-8') as f:
            data = json.load(f)
            #print(data)
            #print(type(data))
            results = data['results']
            dict_list = []
            #提取json文件中的字典写入dict_list
            for i in results:
                dict = {}
                APPLICATION_ID = i['appl_id']
                dict['APPLICATION_ID'] = APPLICATION_ID
                project_title = i['project_title']
                dict['project_title'] = project_title
                dict_list.append(dict.copy())
        batch_insert_sql("nih_title_2020", dict_list)


if __name__ == '__main__':
    conn = pymysql.connect(host='localhost', user='root', password='xx19941130', database='nih')
    if conn:
        print('正常连接')
    cursor = conn.cursor()

    cat_2020_title_insert()