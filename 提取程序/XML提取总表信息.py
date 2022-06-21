from xml.etree import ElementTree as ET
import xlsxwriter
import pymysql
import sys
import os


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


def creat_table():
    # 创建表
    table_list = ["NIH_总表"]
    cursor = conn.cursor()
    for table in table_list:
        str = 'CREATE table {} (id INT primary key NOT NULL AUTO_INCREMENT)'.format(table)
        print(str)
        cursor.execute(str)
        conn.commit()
        if table == "NIH_总表":
            key_list = ['APPLICATION_ID', 'ACTIVITY', 'ADMINISTERING_IC', 'APPLICATION_TYPE', 'ARRA_FUNDED',
                        'AWARD_NOTICE_DATE',
                        'BUDGET_START', 'BUDGET_END', 'CFDA_CODE', 'CORE_PROJECT_NUM', 'ED_INST_TYPE', 'FOA_NUMBER',
                        'FULL_PROJECT_NUM', 'FUNDING_ICs',
                        'FUNDING_MECHANISM', 'FY', 'IC_NAME', 'NIH_SPENDING_CATS', 'ORG_CITY', 'ORG_COUNTRY',
                        'ORG_DEPT',
                        'ORG_DISTRICT', 'ORG_DUNS', 'ORG_FIPS', 'ORG_IPF_CODE', 'ORG_NAME', 'ORG_STATE', 'ORG_ZIPCODE',
                        'PHR', 'PROGRAM_OFFICER_NAME', 'PROJECT_START', 'PROJECT_END', 'PROJECT_TITLE', 'SERIAL_NUMBER',
                        'STUDY_SECTION', 'STUDY_SECTION_NAME',
                        'SUBPROJECT_ID', 'SUPPORT_YEAR', 'SUFFIX', 'DIRECT_COST_AMT', 'INDIRECT_COST_AMT', 'TOTAL_COST',
                        'TOTAL_COST_SUB_PROJECT', ]
            for key in key_list:
                str = 'alter table {} add {} Text'.format(table, key)
                cursor.execute(str)
                conn.commit()
    cursor.close()


if __name__ == '__main__':
    conn = pymysql.connect(host='localhost', user='root', password='a3x6v8f8', database='nih')
    if conn:
        print('正常连接')

    creat_table()

    for i in [2016, 2017, 2018, 2019, 2020]:
        xml = open(r'C:\Users\Administrator\PycharmProjects\NIH分析\数据\X\project\RePORTER_PRJ_X_FY{}_new.xml'.format(i),
                   encoding='utf-8').read()
        root = ET.fromstring(xml)
        print(root.tag)
        child = list(root)
        print(len(child))

        dict_list = []
        count = 0
        for row in child:
            count += 1
            dict = {}
            APPLICATION_ID = row.find('APPLICATION_ID').text
            dict['APPLICATION_ID'] = APPLICATION_ID
            ACTIVITY = row.find('ACTIVITY').text
            dict['ACTIVITY'] = ACTIVITY
            ADMINISTERING_IC = row.find('ADMINISTERING_IC').text
            dict['ADMINISTERING_IC'] = ADMINISTERING_IC
            APPLICATION_TYPE = row.find('APPLICATION_TYPE').text
            dict['APPLICATION_TYPE'] = APPLICATION_TYPE
            ARRA_FUNDED = row.find('ARRA_FUNDED').text
            dict['ARRA_FUNDED'] = ARRA_FUNDED
            AWARD_NOTICE_DATE = row.find('AWARD_NOTICE_DATE').text
            dict['AWARD_NOTICE_DATE'] = AWARD_NOTICE_DATE
            BUDGET_START = row.find('BUDGET_START').text
            dict['BUDGET_START'] = BUDGET_START
            BUDGET_END = row.find('BUDGET_END').text
            dict['BUDGET_END'] = BUDGET_END
            CFDA_CODE = row.find('CFDA_CODE').text
            dict['CFDA_CODE'] = CFDA_CODE
            CORE_PROJECT_NUM = row.find('CORE_PROJECT_NUM').text
            dict['CORE_PROJECT_NUM'] = CORE_PROJECT_NUM
            FOA_NUMBER = row.find('FOA_NUMBER').text
            dict['FOA_NUMBER'] = FOA_NUMBER
            ED_INST_TYPE = row.find('ED_INST_TYPE').text
            dict['ED_INST_TYPE'] = ED_INST_TYPE
            FOA_NUMBER = row.find('FOA_NUMBER').text
            dict['FOA_NUMBER'] = FOA_NUMBER
            FULL_PROJECT_NUM = row.find('FULL_PROJECT_NUM').text
            dict['FULL_PROJECT_NUM'] = FULL_PROJECT_NUM
            FUNDING_ICs = row.find('FUNDING_ICs').text
            dict['FUNDING_ICs'] = FUNDING_ICs
            FUNDING_MECHANISM = row.find('FUNDING_MECHANISM').text
            dict['FUNDING_MECHANISM'] = FUNDING_MECHANISM
            FY = row.find('FY').text
            dict['FY'] = FY
            IC_NAME = row.find('IC_NAME').text
            dict['IC_NAME'] = IC_NAME

            ORG_CITY = row.find('ORG_CITY').text
            dict['ORG_CITY'] = ORG_CITY
            ORG_COUNTRY = row.find('ORG_COUNTRY').text
            dict['ORG_COUNTRY'] = ORG_COUNTRY
            ORG_DEPT = row.find('ORG_DEPT').text
            dict['ORG_DEPT'] = ORG_DEPT
            ORG_DISTRICT = row.find('ORG_DISTRICT').text
            dict['ORG_DISTRICT'] = ORG_DISTRICT
            ORG_DUNS = row.find('ORG_DUNS').text
            dict['ORG_DUNS'] = ORG_DUNS
            ORG_FIPS = row.find('ORG_FIPS').text
            dict['ORG_FIPS'] = ORG_FIPS
            ORG_IPF_CODE = row.find('ORG_IPF_CODE').text
            dict['ORG_IPF_CODE'] = ORG_IPF_CODE
            ORG_NAME = row.find('ORG_NAME').text
            dict['ORG_NAME'] = ORG_NAME
            ORG_STATE = row.find('ORG_STATE').text
            dict['ORG_STATE'] = ORG_STATE
            ORG_ZIPCODE = row.find('ORG_ZIPCODE').text
            dict['ORG_ZIPCODE'] = ORG_ZIPCODE

            PHR = row.find('PHR').text
            dict['PHR'] = PHR
            PROGRAM_OFFICER_NAME = row.find('PROGRAM_OFFICER_NAME').text
            dict['PROGRAM_OFFICER_NAME'] = PROGRAM_OFFICER_NAME
            PROJECT_START = row.find('PROJECT_START').text
            dict['PROJECT_START'] = PROJECT_START
            PROJECT_END = row.find('PROJECT_END').text
            dict['PROJECT_END'] = PROJECT_END

            PROJECT_TITLE = row.find('PROJECT_TITLE').text
            dict['PROJECT_TITLE'] = PROJECT_TITLE
            SERIAL_NUMBER = row.find('SERIAL_NUMBER').text
            dict['SERIAL_NUMBER'] = SERIAL_NUMBER

            STUDY_SECTION = row.find('STUDY_SECTION').text
            dict['STUDY_SECTION'] = STUDY_SECTION
            STUDY_SECTION_NAME = row.find('STUDY_SECTION_NAME').text
            dict['STUDY_SECTION_NAME'] = STUDY_SECTION_NAME
            SUBPROJECT_ID = row.find('SUBPROJECT_ID').text
            dict['SUBPROJECT_ID'] = SUBPROJECT_ID
            SUPPORT_YEAR = row.find('SUPPORT_YEAR').text
            dict['SUPPORT_YEAR'] = SUPPORT_YEAR
            SUFFIX = row.find('SUFFIX').text
            dict['SUFFIX'] = SUFFIX
            DIRECT_COST_AMT = row.find('DIRECT_COST_AMT').text
            dict['DIRECT_COST_AMT'] = DIRECT_COST_AMT
            INDIRECT_COST_AMT = row.find('INDIRECT_COST_AMT').text
            dict['INDIRECT_COST_AMT'] = INDIRECT_COST_AMT
            TOTAL_COST = row.find('TOTAL_COST').text
            dict['TOTAL_COST'] = TOTAL_COST
            TOTAL_COST_SUB_PROJECT = row.find('TOTAL_COST_SUB_PROJECT').text
            dict['TOTAL_COST_SUB_PROJECT'] = TOTAL_COST_SUB_PROJECT
            dict_list.append(dict)

            if count >= 500:
                # 写入总表
                cursor = conn.cursor()
                batch_insert_sql("NIH_总表", dict_list)
                cursor.close()
                count = 0
                dict_list = []

        # 剩下的数组再插一次
        cursor = conn.cursor()
        batch_insert_sql("NIH_总表", dict_list)
        cursor.close()
