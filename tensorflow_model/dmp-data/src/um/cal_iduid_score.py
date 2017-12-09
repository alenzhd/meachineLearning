#!/usr/bin/env python
# -*-coding:utf-8 -*-

import sys,os,string
import traceback
import math

if __name__ == '__main__':
    # lines = ['imei1\tmix_uid1|:|3|:|0.555555|,|mix_uid2|:|7|:|0.666666\tad\timei']
    # for line in lines:
    for line in sys.stdin:
        try:
            temp = line.strip().split("\t")
            #输入：id  mix_uid|:|cnt|:|uid2id_score|,|...  user_type flag
            #输出：mix_uid,id,id2uid_score,uid2id_score,net_type,user_type,flag
            arr = temp[1].replace('"', '').split('|,|')
            if len(arr) > 1000: continue
            total = 0
            for value in arr:
                kv = value.split('|:|')
                if len(kv)==3:
                    total += float(kv[1])
            for value in arr:
                kv = value.split('|:|')
                if len(kv)==3:
                    r = 1.0*float(kv[1])/total
                    print kv[0]+"\t"+temp[0]+"\t"+str(r)+"\t"+kv[2]+"\t"+temp[2]+"\t"+temp[3]
        except Exception, e:
            # print e
            continue
