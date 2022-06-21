f = open('cats-neurosciences.txt','r').readlines()
str = ""
for i in f:
    i = i.replace("\n",'')
    print(i)
    str += '"{}",'.format(i)

print(str)