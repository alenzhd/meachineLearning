#!/usr/bin/env python
# -*-coding:utf-8 -*-
import sys

FLAG = 1
mix_m_uid=' ';
app_id=' ';
site_id=' ';
cont_id=' ';
action_id=' ';
value=' ';
value_type_id=' ';
duration=' ';
last_timestamp=' ';
count =' ';
if __name__ == '__main__':
    for line in sys.stdin:
        try:
            temp = line.strip().split('\t')
            #输入：	mix_m_uid, app_id, site_id, cont_id, action_id, value, value_type_id, duration, last_timestamp, count
            #下标：     0         1       2        3        4         5        6             7          8             9
            if FLAG == 1:
                mix_m_uid = temp[0]
                app_id = temp[1]
                site_id = temp[2]
                cont_id = temp[3]
                action_id = temp[4]
                value = temp[5]
                value_type_id = temp[6]
                duration = temp[7]
                last_timestamp = temp[8]
                count = temp[9]
                FLAG = 0
                continue
            if temp[0] == mix_m_uid and abs(int(last_timestamp) - int(temp[8])) < 60000 and value[0] == temp[5][0]:
                pass
            else:
                print mix_m_uid + '\t' + app_id + '\t'+ site_id + '\t' + cont_id + '\t' + action_id+ '\t' + value + '\t'+value_type_id + '\t' + duration + '\t' + last_timestamp+ '\t' + count
            mix_m_uid = temp[0]
            app_id = temp[1]
            site_id = temp[2]
            cont_id = temp[3]
            action_id = temp[4]
            value = temp[5]
            value_type_id = temp[6]
            duration = temp[7]
            last_timestamp = temp[8]
            count = temp[9]
        except Exception, e:
            print e
            continue
    print mix_m_uid + '\t' + app_id + '\t' + site_id + '\t' + cont_id + '\t' + action_id + '\t' + value + '\t' + value_type_id + '\t' + duration + '\t' + last_timestamp + '\t' + count
