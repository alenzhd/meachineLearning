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
            arrs = temp[1].split("|")
            dic = dict()
            for arr in arrs:
                arr_temp = arr.split(",")
                dic[arr_temp[2]]=(arr_temp[0],arr_temp[1])
            t = sorted(dic.iteritems(),key=lambda d:d[0])
            n = len(t)
            ip_daycount = Caltime(t[0][0],t[n-1][0])+1
            ip_maxDuration=0
            ip_medianDuration=0
            ip_count=0
            if (n > 0):
                ip = []
                count = []
                duration = []
                day_tmp = t[0][0]
                ip_tmp = t[0][1][0]
                count_tmp = int(t[0][1][1])
                ip.append(ip_tmp)
                count.append(count_tmp)
                for i in range(1,n):
                    this_day = t[i][0]
                    this_ip = t[i][1][0]
                    this_count = int(t[i][1][1])
                    ip.append(this_ip)
                    count.append(this_count)
                    if this_ip==ip_tmp and this_count==1 and count_tmp==1:
                        continue
                    else:
                        duration.append(Caltime(day_tmp,this_day))
                        day_tmp = this_day
                        ip_tmp = this_ip
                duration.append(Caltime(day_tmp,t[n-1][0])+1)
                ip_maxDuration = max(duration)
                ip_medianDuration = duration[len(duration)//2]
                ip_count=sum(count)-n+len(set(ip))
            print mix_uid+"\t"+"0\t0\t0\t"+str(ip_count)+"\t"+str(ip_maxDuration)+"\t"+str(ip_medianDuration)+"\t"+str(ip_daycount)
        except Exception, e:
            continue
