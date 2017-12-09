#!/usr/bin/env python
# coding:utf-8
import os
import sys
import re
reload(sys)  
sys.setdefaultencoding('utf8')

current_path=os.path.dirname(os.path.abspath(__file__))
print 'current_path:'+current_path
SOURCE_TB_PATH =current_path+'/source_tb/'

class Brand(object):
    id = 0
    position = 0
    enName = ''
    chName = ''
    siteBrandName = ''
    site = ''
    newId = ''

def getBrand(line):
    brand = Brand()
    sl = line.split("\t")
    if len(sl) != 3:
        return
    brand.id = sl[0]
    name = sl[1]
    brand.site = sl[2]
    brand.siteBrandName = name
    strInfo = re.compile(u'([\\)]|\xef\xbc\x89)')
    name = strInfo.sub('', name)
    spn = re.split('\\(|/|\xef\xbc\x88|\|', name)
    if len(spn) == 1:
        fuZhi(brand, spn[0])
    elif len(spn) == 2:
        fuZhi(brand, spn[0])
        fuZhi(brand, spn[1])
    elif len(spn) > 2:
        n = "-".join(spn[1:])
        fuZhi(brand, spn[0])
        fuZhi(brand, n)
    return brand

def fuZhi(brand, name):
    patterntotal = re.compile(u'[\u4e00-\u9fa5]+')
    cn = patterntotal.search(name.decode('utf-8'))
    if cn:
        brand.chName = name
    else:
        brand.enName = name

def handle(brandList):
    for index in range(len(brandList)):
        a = brandList[index]
        if index != a.position:
            continue
        n = index+1
        for j in range(n,len(brandList)):
            b = brandList[j]
            if b.position != j:
                continue
            if (not isEqual(a.enName,b.enName)) and (not isEqual(a.chName,b.chName)):
                continue
            if(a.enName == '' or a.chName == '') and (b.enName != '' and b.chName != ''):
                a.position = j
                break
            else:
                b.position = index

def isEqual(n1,n2):
    if n1 == '' or n2 == '':
        return False
    a = n1.upper()
    b = n2.upper()
    return a == b

def addNewId(brandList):
    newId =0
    for i in range(len(brandList)):
        brand = brandList[i]
        if brand.position == i:
            newId += 1
            brand.newId =str(newId)

def find(list,i):
    while list[i].position != i:
        i = list[i].position
    return i

if __name__ == '__main__':
    reader_site_brand = open(SOURCE_TB_PATH +"//get_site_brands.txt")
    writer_std_brands = open(SOURCE_TB_PATH +"//site_std_brand.txt",'w')
    writer_brand_base = open(SOURCE_TB_PATH +"//site_data_brand_base.txt",'w')

    brandList = []
    for sline in reader_site_brand:
        brand = Brand()
        brand = getBrand(sline)
        if brand is not None:
            brand.position = len(brandList)
            brandList.append(brand)

    handle(brandList)
    #addNewId(brandList)

    for i in range(len(brandList)):
        brands = brandList[i]
        p = brands.position
        if p == i:
            if (brands.chName == '') and (brands.enName != ''):
                writer_std_brands.write(str(brands.position)+"\t"+brands.enName+"\n")
            elif (brands.enName == '') and (brands.chName != ''):
                writer_std_brands.write(str(brands.position)+"\t"+brands.chName+"\n")
            else:
                writer_std_brands.write(str(brands.position)+"\t"+brands.chName+"/"+brands.enName+"\n")
    for i in range(len(brandList)):
        cur = brandList[i]
        std = brandList[find(brandList,i)]
        if (std.chName == '') and (std.enName != ''):
            writer_brand_base.write(cur.id+"\t"+cur.siteBrandName+"\t"+str(std.position)+"\t"+std.enName+"\t"+cur.site)
        elif (std.enName == '') and (std.chName != ''):
            writer_brand_base.write(cur.id+"\t"+cur.siteBrandName+"\t"+str(std.position)+"\t"+std.chName+"\t"+cur.site)
        else:
            writer_brand_base.write(cur.id+"\t"+cur.siteBrandName+"\t" \
                                    + str(std.position) + "\t" + std.chName + "/" + std.enName + "\t" + cur.site)

    reader_site_brand.close()
    writer_brand_base.close()
    writer_std_brands.close()






