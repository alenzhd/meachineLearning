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
    #输入：mix_uid, flag, hour_list
    #输出：mix_uid, flag, weight, hour_list
    # lines = ['m\tf\t0.1\t000000000100000000010000','m\tf\t0.1\t100000000000000000000001', 'm1\tf\t0.1\t000000000100000000010000', 'm\tf1\t0.1\t000000000100000000010000']
    exist_mix_uid, exist_flag = '',''
    exist_hour_list = '000000000000000000000000'
    # for line in lines:
    for line in sys.stdin:
        try:
            mix_uid, flag, weight, hour_list = line.strip().split('\t')
            if(exist_mix_uid != '' and (exist_mix_uid != mix_uid or exist_flag != flag)):
                print exist_mix_uid + '\t' + exist_flag + '\t' + str(exist_hour_list.count('1')) + '\t' + exist_hour_list
                exist_hour_list = hour_list
            else:
                exist_hour_list = deal_hour_list(exist_hour_list, hour_list)
            exist_mix_uid , exist_flag, tmp1, tmp2 = line.strip().split('\t')
        except Exception, e:
            # print e
            continue
    print exist_mix_uid + '\t' + exist_flag + '\t' + str(exist_hour_list.count('1')) + '\t' + exist_hour_list