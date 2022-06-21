import xlrd
from collections import Counter
import xlsxwriter
import nltk
from nltk.stem.wordnet import WordNetLemmatizer

def get_wordnet_pos(treebank_tag):
    if treebank_tag.startswith('J'):
        return nltk.corpus.wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return nltk.corpus.wordnet.VERB
    elif treebank_tag.startswith('N'):
        return nltk.corpus.wordnet.NOUN
    elif treebank_tag.startswith('R'):
        return nltk.corpus.wordnet.ADV
    else:
        return ''

data = xlrd.open_workbook('140terms.xlsx')#文件名以及路径，如果路径或者文件名有中文给前面加一个r拜师原生字符。
table = data.sheets()[0]
col = table.col_values(1,start_rowx=1, end_rowx=None)
print(len(col))
all_words = []
for i in col:
    each_list = i.split(';')
    #print(each_list)
    for a in each_list:
        #print(a)
        a = a.lower() #字母最小化处理
        a = a.lstrip() #去除前空格
        a = a.rstrip() #去除后空格
        #print(a)
        all_words.append(a)
#去除掉中间的空值
while '' in all_words:
    all_words.remove('')

#全单词
print('全单词')
print(all_words)
print(len(all_words))
set_all_words = list(set(all_words))
print('去重后的单词')
print(set_all_words)
print(len(set_all_words))
#词频统计
wordcount = Counter(all_words)
wordcount_list = wordcount.most_common(1400)
print(wordcount_list)
# 新建工作簿
work_book = xlsxwriter.Workbook('未还原时态.xls')
# 增加sheet表
work_sheet = work_book.add_worksheet('sheet1')
c = 0
for i in wordcount_list:
    #print(i[0],i[1])
    #print(i[0])
    work_sheet.write(0+c, 0, i[0])
    work_sheet.write(0+c, 1, i[1])
    c += 1
work_book.close()


lmtzr_list = []
#根据TAG还原时态
tag = nltk.pos_tag(all_words)
print('tag情况')
print(tag)
for i in tag:
    text= i[0]
    pos = i[1]
    HY = get_wordnet_pos(pos)
    print(text)
    print(pos,HY)
    lmtzr = WordNetLemmatizer()
    if HY != '':
        new_word = lmtzr.lemmatize(text, HY)
        print(new_word)
        lmtzr_list.append(new_word)

#词频统计
wordcount = Counter(lmtzr_list)
wordcount_list = wordcount.most_common(1400)
print(wordcount_list)
# 新建工作簿
work_book = xlsxwriter.Workbook('还原时态.xls')
# 增加sheet表
work_sheet = work_book.add_worksheet('sheet1')
c = 0
for i in wordcount_list:
    #print(i[0],i[1])
    #print(i[0])
    work_sheet.write(0+c, 0, i[0])
    work_sheet.write(0+c, 1, i[1])
    work_sheet.write(0+c, 2, i[1])
    c += 1
work_book.close()