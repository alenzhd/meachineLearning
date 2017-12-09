#!/usr/bin/env python
# -*-coding:utf-8 -*-
import sys

recode = ' '
if __name__ == '__main__':
    for line in sys.stdin:
        try:
            temp = line.strip().split('\t')
            #输入：a.type_id, a.tags, b.weight, c.mix_uid, b.flag, c.weight, b.flag_id
            #下标：     0        1       2           3       4        5          6
            #输出1：a.type_id,a.tags,b.weight
            #输出2：'i', (AD|UU)c.mix_uid, c.weight
            if temp[6] != recode:
                recode = temp[6]
                if temp[4] == 'ip':
                    print temp[6] + '\t' + 'i' + '\t' + 'AD' + temp[3] + '\t' + temp[5] + '\t' + temp[4]
                else:
                    print temp[6] + '\t' + 'i' + '\t' + 'UU' + temp[3] + '\t' + temp[5] + '\t' + temp[4]
                print temp[6] + '\t' + temp[0] + '\t' + temp[1] + '\t' + temp[2] + '\t' + temp[4]
            else:
                print temp[6] + '\t' + temp[0] + '\t' + temp[1] + '\t' + temp[2] + '\t' + temp[4]
        except Exception, e:
            continue
