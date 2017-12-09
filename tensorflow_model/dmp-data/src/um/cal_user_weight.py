#!/usr/bin/env python
# -*-coding:utf-8 -*-

import sys,os,string
import traceback
import math
import datetime
    
def Caltime(date1,date2):
    d1 = datetime.datetime(int(date1[0:4]), int(date1[4:6]), int(date1[6:8]))
    d2 = datetime.datetime(int(date2[0:4]), int(date2[4:6]), int(date2[6:8]))
    return (d2 - d1).days

if __name__ == '__main__':
    #c.uid,c.w1,c.w2,c.createdate,c.lastdate,c.today,c.usertype
    #当天weight，累计weight，diffdate=c.today-c.lastdate 
    for line in sys.stdin:
        try:
            temp = line.strip().split("\t")
            mix_uid = temp[0]
            total_weight = temp[1]
            new_weight = temp[2]
            create_date = temp[3]
            last_date = temp[4]
            today = temp[5]
            user_type = temp[6]
            if last_date =='none':
                #mix_uid,weight,create_date,last_date,user_type
                print mix_uid+"\t"+new_weight+"\t"+create_date+"\t"+today+"\t"+user_type
            elif new_weight == 'none' or today == 'none':
                print mix_uid+"\t"+total_weight+"\t"+create_date+"\t"+last_date+"\t"+user_type
            else:
                diff_date = Caltime(last_date,today)
                if user_type == 'mobile':
                    multi1 = -0.2
                    divide = 1.0/((math.exp(multi1*29)+1)*math.exp(multi1*29)/2)
                    r_weight = divide * float(new_weight) + math.exp(multi1*int(diff_date)) * float(total_weight)
                    print mix_uid+"\t"+str(r_weight)+"\t"+create_date+"\t"+today+"\t"+user_type
                else:
                    multi2 = -0.8
                    r_other_weight = 1.0 * float(new_weight) + math.exp(multi2*int(diff_date)) * float(total_weight)
                    print mix_uid+"\t"+str(r_other_weight)+"\t"+create_date+"\t"+today+"\t"+user_type
        except Exception, e:
            continue

