#!/usr/bin/env python
# -*-coding:utf-8 -*-

import sys,os,string
import traceback
import math

def deal_hour_list(v1_hourlist, v2_hourlist):
    res = ''
    for i in range(24):
        if v1_hourlist[i] == '1' or v2_hourlist[i] == '1':
            res += '1'
        else:
            res += '0'
    return  res

if __name__ == '__main__':
    #输入：mix_uid, type_id, tags, hour_list, user_type
    #输出：mix_uid, type_id, tags, weight, hour_list, user_type
    # lines = ['m\t1\t11\t000000000100000000010000\tad','m\t0\t11\t100000000000000000000001\tad', 'm1\t0\t12\t000000000100000000010000\tmobile', 'm1\t0\t12\t000001000100000000010000\tmobile']
    exist_mix_uid, exist_type_id, exist_tags, exist_user_type = '','','',''
    exist_hour_list = '000000000000000000000000'
    # for line in lines:
    for line in sys.stdin:
        try:
            mix_uid, type_id, tags, hour_list, user_type = line.strip().split('\t')
            if(exist_mix_uid != '' and (exist_mix_uid != mix_uid or exist_type_id != type_id or exist_tags != tags or exist_user_type != user_type)):
                print exist_mix_uid + '\t' + exist_type_id + '\t' + exist_tags + '\t' + str(exist_hour_list.count('1')) + '\t' + exist_hour_list + '\t' + exist_user_type
                exist_hour_list = hour_list
            else:
                exist_hour_list = deal_hour_list(exist_hour_list, hour_list)
            exist_mix_uid, exist_type_id, exist_tags, tmp, exist_user_type = line.strip().split('\t')
        except Exception, e:
            # print e
            continue
    print exist_mix_uid + '\t' + exist_type_id + '\t' + exist_tags + '\t' + str(exist_hour_list.count('1')) + '\t' + exist_hour_list + '\t' + exist_user_type