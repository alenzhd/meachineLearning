#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  4 16:21:39 2015

@author: zhangqn
"""

import sys

def isNum(value):
    try:
        float(value)
    except ValueError:
        return False
    else:
        return True
    
try:
    for line in sys.stdin:
        temp = line.strip().split("\t")
        #m_id
        #phone_list
        #mflag
        tmp = temp[1].split("_")
        tmp=tmp[len(tmp)-1].split(",")
        lon,lat=0.0,0.0
        if len(tmp)!=2 : 
            continue
        if not isNum(tmp[0]) or not isNum(tmp[1]):
            continue
        tmp=[float(tmp[0]),float(tmp[1])]
        if tmp[0]>90 and tmp[0]<180 and tmp[1]<90 and tmp[1]>0:
            lon = tmp[0]
            lat = tmp[1]
        elif tmp[1]>90 and tmp[1]<180 and tmp[0]<90 and tmp[0]>0:
            lon = tmp[1]
            lat = tmp[0]
        else:
            continue
        print (temp[0]+"\t"+str(lon)+","+str(lat))
except Exception, e:
    print traceback.format_exc()
    
