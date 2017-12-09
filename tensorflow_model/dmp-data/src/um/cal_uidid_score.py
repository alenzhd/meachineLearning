#!/usr/bin/env python
# -*-coding:utf-8 -*-

import sys,os,string
import traceback
import math

if __name__ == '__main__':
    # lines = ['mix_uid1\timei1|:|3|,|imei2|:|7|,|imei3|:|10\tad\timei', 'mix_uid2\timei4|:|3\tad\timei']
    # for line in lines:
    for line in sys.stdin:
        try:
            temp = line.strip().split("\t")
            #输入：mix_uid   id|:|weight|,|id|:|weight  user_type flag
            #输出：mix_uid,id,weight,uid2id_score,net_type,user_type,flag
            arr = temp[1].replace('"', '').split('|,|')
            if len(arr) > 1000:
                continue
            total = 0
            for value in arr:
                kv = value.split('|:|')
                if len(kv) == 3:
                    total += float(kv[1])
            for value in arr:
                kv = value.split('|:|')
                if len(kv) == 3:
                    r = 1.0*float(kv[1])/total
                    print temp[0]+"\t"+kv[0]+"\t"+kv[1]+"\t"+str(r)+"\t"+temp[2]+"\t"+temp[3]
        except Exception, e:
            # print e
            continue


