#!/usr/bin/env python
# -*-coding:utf-8 -*-

import sys
import os

reload(sys)  
sys.setdefaultencoding('utf8')

if __name__ == '__main__':
    lineid = 0
    for line in sys.stdin:
        try:
            lineid+=1
            temp = line.strip().split("\t")
            maxid = temp[0]
            std_brand_name = temp[1]
            position = temp[2]
            newid = int(maxid)+int(lineid)
            rs = str(newid)+"\t"+std_brand_name+"\t"+position
            print rs
        except Exception, err:
            continue
        
    
