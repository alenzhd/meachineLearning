#!/usr/bin/env python
# -*-coding:utf-8 -*-

import os,sys
import datetime
import time
import redis

dates = sys.argv[1]


while True:
        r = redis.Redis(host='10.1.1.100',port=6399,db=0)
        list = r.keys("dxy_u_e_*_"+dates)
        if  len(list) >= 8:
            #r.delete(key)
            print "已有超过8个省份完成:"+str(len(list))
            sys.stdout.flush()
            break
        else:
            print "还不足8个省份:"+str(len(list))
            sys.stdout.flush()
            #休眠五分钟
            time.sleep(10) 
            continue

