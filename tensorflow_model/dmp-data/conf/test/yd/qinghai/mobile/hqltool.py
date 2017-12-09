#!/usr/bin/env python
# -*-coding:utf-8 -*-
import time
import math
import os,sys

ISOTIMEFORMAT='%Y-%m-%d %X'
mix_conf = os.path.dirname(os.path.abspath(__file__))
mix_home = mix_conf +'/../../../../../'
lock_path = mix_home+'./locks/'

print sys.path

from setting import *

print HIVE_DATABASE

START_TIME = 0.0

#客户端运行hql
def HiveExe1(hql,name,dates):
    filename = name+'_'+dates
    cmd = "hive -e \" use "+HIVE_DATABASE+";set mapreduce.job.queuename="+QUEUE+";"
    for sql in hql:
        cmd = cmd + sql + ";"
        print time.strftime( ISOTIMEFORMAT, time.localtime() )
    cmd =cmd + "\""
    print cmd
    sys.stdout.flush()
    status = os.system(cmd)
    if status != 0 :
        #sys.exit(status)
        return 1
    else :
        return 0
#客户端运行hql
def HiveExe(hql,name,dates):
    cmd = "hive -e \" use "+HIVE_DATABASE+";set mapreduce.job.queuename="+QUEUE+";"
    for sql in hql:
        cmd = cmd + sql + ";"
        print time.strftime( ISOTIMEFORMAT, time.localtime() )
    cmd =cmd + "\""
    
    maxCount = 3  #最大重试次数
    status = 1
    count = 1
    while status != 0 and count <= maxCount:
        print cmd 
        sys.stdout.flush()
        status = os.system(cmd)
        if status == 0 :
            return 0
        tmpLog = '第'+str(count)+'执行错误，重新执行！'
        print tmpLog
        count = count + 1
    else:
    	tmpLog = '执行次数超过最大重试次数【'+str(maxCount)+'】，退出执行！'
        print tmpLog
        return 1

#日期参数处理
def Pama(dicts,dates):
    today = time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))
    today_iso = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    import datetime
    year = str(dates[0:4])
    month = str(dates[4:6])
    if len(dates) >= 8:
        day = str(dates[6:8])
        hour = str(dates[8:10])
        days = datetime.date(int(dates[0:4]),int(dates[4:6]),int(dates[6:8]))
    else:
        day = '01'
        hour = '00'
        days = datetime.date(int(dates[0:4]),int(dates[4:6]),1)
    lastday = datetime.date(days.year,days.month,days.day) - datetime.timedelta(1)
    last7day = datetime.date(days.year,days.month,days.day) - datetime.timedelta(7)
    last30day = datetime.date(days.year,days.month,days.day) - datetime.timedelta(30)
    lastmonend = datetime.date(days.year,days.month,01) - datetime.timedelta(1)
    last2monend = datetime.date(lastmonend.year,lastmonend.month,01) - datetime.timedelta(1)
    last3monend = datetime.date(last2monend.year,last2monend.month,01) - datetime.timedelta(1)
    monday = days-datetime.timedelta(days=days.weekday())
    sunday = days+datetime.timedelta(days=6-days.weekday())
    #上月同日期
    if days.month == 3:
        if divmod(days.year,4)[1] == 0:
            if days.day == 31 or days.day == 30:
                lastmon = datetime.date(lastmonend.year,lastmonend.month,29)
            else:
                lastmon = datetime.date(lastmonend.year,lastmonend.month,days.day)
        else:
            if days.day == 31 or days.day == 30 or days.day ==29:
                lastmon = datetime.date(lastmonend.year,lastmonend.month,28)
            else:
                lastmon = datetime.date(lastmonend.year,lastmonend.month,days.day)
    else:
        if days.month in (5,7,10,12):
            if days.day == 31:
              lastmon = datetime.date(lastmonend.year,lastmonend.month,30)
            else:
              lastmon = datetime.date(lastmonend.year,lastmonend.month,days.day)
        else:
            lastmon = datetime.date(lastmonend.year,lastmonend.month,days.day)
    #上2月同日期
    if days.month == 4:
        if divmod(days.year,4)[1] == 0:
            if days.day == 31 or days.day == 30:
                last2mon = datetime.date(last2monend.year,last2monend.month,29)
            else:
                last2mon = datetime.date(last2monend.year,last2monend.month,days.day)
        else:
            if days.day == 31 or days.day == 30 or days.day ==29:
                last2mon = datetime.date(last2monend.year,last2monend.month,28)
            else:
                last2mon = datetime.date(last2monend.year,last2monend.month,days.day)
    else:
        if days.month in (1,8):
            if days.day == 31:
              last2mon = datetime.date(last2monend.year,last2monend.month,30)
            else:
              last2mon = datetime.date(last2monend.year,last2monend.month,days.day)
        else:
            last2mon = datetime.date(last2monend.year,last2monend.month,days.day)
    #上3月同日期
    if days.month == 5:
        if divmod(days.year,4)[1] == 0:
            if days.day == 31 or days.day == 30:
                last3mon = datetime.date(last3monend.year,last3monend.month,29)
            else:
                last3mon = datetime.date(last3monend.year,last3monend.month,days.day)
        else:
            if days.day == 31 or days.day == 30 or days.day ==29:
                last3mon = datetime.date(last3monend.year,last3monend.month,28)
            else:
                last3mon = datetime.date(last3monend.year,last3monend.month,days.day)
    else:
        if days.month in (7,12):
            if days.day == 31:
                last3mon = datetime.date(last3monend.year,last3monend.month,30)
            else:
                last3mon = datetime.date(last3monend.year,last3monend.month,days.day)
        else:
            last3mon = datetime.date(last3monend.year,last3monend.month,days.day)
    #上月的第一天
    lastmon01 = datetime.date(lastmonend.year,lastmonend.month,1)
    #本月最后一天
    if month == '12':
        monend = datetime.date(int(year)+1,1,1) - datetime.timedelta(days=1)
    else:
        monend = datetime.date(int(year),int(month)+1,1) - datetime.timedelta(days=1)
    #上年同日期
    lastyearend = datetime.date(days.year,01,01) - datetime.timedelta(1)
    if divmod(days.year,4)[1] == 0:
        if days.month == 2 and days.day == 29:
            lastyear = datetime.date(lastyearend.year,02,28)
        else:
            lastyear = datetime.date(lastyearend.year,days.month,days.day)
    else:
        lastyear = datetime.date(lastyearend.year,days.month,days.day)

    dicts['ARG_TODAY'] = today                                      #获得yyyymmddhh格式的当前日期
    dicts['ARG_TODAY_ISO'] = today_iso                              #获得yyyy-mm-dd hh格式的当前日期
    dicts['ARG_OPTIME'] = days.strftime('%Y%m%d')                   #获得yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_ISO'] = days                                  #获得yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_YEAR'] = year                                 #获得yyyy格式的数据日期
    dicts['ARG_OPTIME_MONTH'] = year+month                          #获得yyyymm格式的数据日期
    dicts['ARG_OPTIME_LASTMONTH'] = lastmon.strftime('%Y%m')        #获得yyyymm格式的上月数据日期
    dicts['ARG_OPTIME_LASTMONTH_ISO'] = lastmon.strftime('%Y-%m')   #获得yyyy-mm格式的上月数据日期
    dicts['ARG_OPTIME_MONTH01'] = year+"-"+'01'                     #获得传入的数据日期的当年第1个月yyyy-mm格式的数据日期
    dicts['ARG_OPTIME_MONTH12'] = year+"-"+'12'                     #获得传入的数据日期的当年第12个月yyyy-mm格式的数据日期
    dicts['ARG_OPTIME_HOUR'] = year+month+day+hour                  #获得yyyymmddhh格式的数据日期
    dicts['ARG_OPTIME_HOUR_STD'] = hour                             #获得hh格式的数据日期
    dicts['ARG_OPTIME_DAY'] = day                                   #获得dd格式的数据日期
    dicts['ARG_OPTIME_THISMON'] = month                             #获得mm格式的数据日期
    dicts['ARG_OPTIME_LASTDAY'] = lastday.strftime('%Y%m%d')        #获得传入的数据日期的前一天yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LASTDAY_ISO'] = lastday                       #获得传入的数据日期的前一天yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LAST7DAY'] = last7day.strftime('%Y%m%d')      #获得传入的数据日期的七天前yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LAST7DAY_ISO'] = last7day                     #获得传入的数据日期的七天前yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LASTMON'] = lastmon.strftime('%Y%m%d')        #获得传入的数据日期的上月同期日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LASTMON_ISO'] = lastmon                       #获得传入的数据日期的上月同期日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LAST2MON'] = last2mon.strftime('%Y%m%d')      #获得传入的数据日期的上2月同期日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LAST2MON_ISO'] = last2mon                     #获得传入的数据日期的上2月同期日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LAST2MONTH'] = last2mon.strftime('%Y%m')      #获得传入的数据日期的上2月yyyymm格式的数据日期
    dicts['ARG_OPTIME_LAST2MONTH_ISO'] = last2mon.strftime('%Y-%m') #获得传入的数据日期的上2月yyyy-mm格式的数据日期
    dicts['ARG_OPTIME_LAST3MON'] = last3mon.strftime('%Y%m%d')      #获得传入的数据日期的上3月同期日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LAST3MON_ISO'] = last3mon                     #获得传入的数据日期的上3月同期日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LAST3MONTH'] = last3mon.strftime('%Y%m')      #获得传入的数据日期的上3月yyyymm格式的数据日期
    dicts['ARG_OPTIME_LAST3MONTH_ISO'] = last3mon.strftime('%Y-%m') #获得传入的数据日期的上3月yyyy-mm格式的数据日期
    dicts['ARG_OPTIME_LASTYEAR'] = lastyear.strftime('%Y%m%d')      #获得传入的数据日期的去年同期日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LASTYEAR_ISO'] = lastyear                     #获得传入的数据日期的去年同期日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LASTYEARMON'] = lastyear.strftime('%Y%m')     #获得传入的数据日期的去年同月yyyymm格式的数据日期
    dicts['ARG_OPTIME_LASTYEARMON_ISO'] = lastyear.strftime('%Y-%m')#获得传入的数据日期的去年同月yyyy-mm格式的数据日期
    dicts['ARG_OPTIME_YEAR01'] = year+'0101'                        #获得传入的数据日期的当年第一天日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_YEAR01_ISO'] = year+"-"+'01-01'               #获得传入的数据日期的当年第一天日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_YEAR12'] = year+'1231'                        #获得传入的数据日期的当年最后一天日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_YEAR12_ISO'] = year+"-"+'12-31'               #获得传入的数据日期的当年最后一天日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_MON01'] = year+month+'01'                     #获得传入的数据日期的本月第一天日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_MON01_ISO'] = year+"-"+month+"-"+'01'         #获得传入的数据日期的本月第一天日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LASTMON01'] = lastmon01.strftime('%Y%m%d')    #获得传入的数据日期的上月第一天日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LASTMON01_ISO'] = lastmon01                   #获得传入的数据日期的上月第一天日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_MONEND'] = monend.strftime('%Y%m%d')          #获得传入的数据日期的本月最后一天日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_MONEND_ISO'] = monend                         #获得传入的数据日期的本月最后一天日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LASTMONEND'] = lastmonend.strftime('%Y%m%d')  #获得传入的数据日期的上月最后一天日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_LASTMONEND_ISO'] = lastmonend                 #获得传入的数据日期的上月最后一天日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_MONDAY'] = monday.strftime('%Y%m%d')          #获得传入的数据日期所在周的周一日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_MONDAY_ISO'] = monday                         #获得传入的数据日期所在周的周一日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_SUNDAY'] = sunday.strftime('%Y%m%d')          #获得传入的数据日期所在周的周日日期yyyymmdd格式的数据日期
    dicts['ARG_OPTIME_SUNDAY_ISO'] = sunday                         #获得传入的数据日期所在周的周日日期yyyy-mm-dd格式的数据日期
    dicts['ARG_OPTIME_LASTDAYMON'] = lastday.strftime('%Y%m')       #获得传入的数据日期的前一天所在月份yyyymm格式的数据日期
    dicts['ARG_OPTIME_LASTDAYMON_ISO'] = lastday.strftime('%Y-%m')  #获得传入的数据日期的前一天所在月份yyyy-mm格式的数据日期
    dicts['ARG_MONTH_DAYS'] = int(lastday.day)                      #获得传入的数据日期的前一天天数

    dicts['ARG_30DAYS_AGO'] = last30day.strftime('%Y%m%d')                      #获得传入的数据的前30天日期
    return dicts

#写开始锁文件
def Start(name,dates):
    filename = name+'_'+dates
    print lock_path+filename
    print time.strftime( ISOTIMEFORMAT, time.localtime() )
    e = os.path.isfile(r'%s' % (lock_path+filename))
    if e is False:
        f = open(r"%s" % (lock_path+filename),'w')
        f.close()
        global START_TIME
        START_TIME = time.time()
        print START_TIME
        print "开始执行："+lock_path+filename
        return 0
    else:
        print "已存在正在运行的其它实例,正在退出..."
        print "删除文件锁：rm "+lock_path+filename
        return 1

def SqlImp(name):
    user='cdh42'
    pwd='cdh42'
    host='10.1.253.184'
    db='test'
    s='mysqlimport -u '+user+' -p'+pwd+' -h '+host+' --local '+db+' '+name
    print s
    os.system(s)
    print 'imp end'
    #sys.exit(1)
    return 1

#异常处理
def Except(name,dates,e):
    filename = name+'_'+dates
    print e
    os.remove('%s' % (lock_path+filename))
    print '程序异常，正在退出...'
    #sys.exit(1)
    return 1

#程序结束，删除锁文件
def End(name,dates):
    filename = name+'_'+dates
    print time.strftime( ISOTIMEFORMAT, time.localtime() )
    os.remove('%s' % (lock_path+filename))
    print START_TIME
    curtime = time.time()
    print curtime
    costtime = curtime - START_TIME;
    print filename + " COST: "+str(costtime)
    print "Successful!"
    return 0

if __name__ == '__main__':
    print mix_home

