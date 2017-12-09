#!/usr/bin/env python
# -*-coding:utf-8 -*-
import sys

FLAG = 1
flag_id = ' '
type_id = ' '
tags = ' '
weight = float(0)
if __name__ == '__main__':
    for line in sys.stdin:
        try:
            temp = line.strip().split('\t')
            #输入：	flag_id,  type_id, tags,  weight
            #下标：  0     1       2       3      
            if FLAG == 1:
                flag_id = temp[0]
                type_id = temp[1]
                tags = temp[2]
                weight = float(temp[3])
                FLAG = 0
                continue        
            if temp[0] == flag_id and temp[1] == type_id and temp[2] == tags:
                weight += float(temp[3])
                continue
            else:
                print flag_id + '\t' + type_id + '\t' + tags + '\t' + str(weight) + '\t' + 'c_ipinyou' 
                flag_id = temp[0]
                type_id = temp[1]
                tags = temp[2]
                weight = float(temp[3])  
        except Exception, e:
            continue
    print flag_id + '\t' + type_id + '\t' + tags + '\t' + str(weight) + '\t' + 'c_ipinyou'  

