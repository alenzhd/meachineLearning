#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：用户累计表（30天），储存所有用户。
# **            cookie用户如果7天未出现，则丢弃；其他用户如果30天未出现，则丢弃。
# **  输入表：  dmp_um_user_bd
# **  输出表:   dmp_um_user_dt
# **  创建者:   guojy
# **  创建日期: 2016/03/09
# **  修改日志:
# **  修改日期: yyyy/mm/dd，修改人 xxx  ，修改内容：
# ** ---------------------------------------------------------------------------------------
# **
# ** ---------------------------------------------------------------------------------------
# **
# **  程序调用格式：python ......py yyyymmdd(hh)
# **
#********************************************************************************************
# **  Copyright(c) 2013 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
#********************************************************************************************

import sys,os
import datetime
import time
import subprocess
#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../'
python_path = []
python_path.append(mix_home+'conf')
sys.path[:0]=python_path
print sys.path

#引入自定义包
from settings import *

def get_date_of_back_someday(day_id,time_length):
    format="%Y%m%d"
    t = time.strptime(day_id, "%Y%m%d")
    result=datetime.datetime(*time.strptime(str(day_id),format)[:6])-datetime.timedelta(days=int(time_length))
    return result.strftime(format)

#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
dates = sys.argv[4]
province = PROVINCE
provider = PROVIDER


try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']
    ARG_OPTIME_LASTDAY = dicts['ARG_OPTIME_LASTDAY']
#===========================================================================================
#自定义变量声明---源表声明
#===========================================================================================
    source_tb = "dmp_um_user_bd"
#===========================================================================================
#自定义变量声明---目标表声明
#===========================================================================================
    target_tb = "dmp_um_user_dt"

#===========================================================================================
#因采取省份轮休，获取最近一天的日期
#===========================================================================================
    filepath = HIVE_TB_HOME + '/' + target_tb + '/provider='+provider+'/province='+province+'/net_type='+NET_TYPE
    cat = subprocess.Popen(['hadoop', 'fs', '-du',filepath],stdout=subprocess.PIPE)
    res = ARG_OPTIME_LASTDAY
    for line in cat.stdout:
        if 'day_id=' in line and int(line.split(' ')[0].strip()) >= 1048576:
            tmp = line.split('=')[-1].strip()
            if int(tmp) < int(ARG_OPTIME):
                res = tmp
    ARG_OPTIME_DATA_LASTDAY = res.strip()
    
#===========================================================================================
#创建创建目标表
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            mix_uid   string,
            weight    double,
            create_date    string,
            last_date    string
        )
        partitioned by (provider string,province string,net_type string,day_id string,user_type string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)

#===========================================================================================
#生成用户累计表：
#cookie用户如果7天未出现，则丢弃；其他用户如果30天未出现，则丢弃。
#===========================================================================================
    back_day = get_date_of_back_someday(ARG_OPTIME, 29)
    back_sevenday = get_date_of_back_someday(ARG_OPTIME, 6)
    back_180_day = get_date_of_back_someday(ARG_OPTIME, 179)
    #累计表=调用日期+最近一天的累计数据
    #查询最近的累计表完成日期
    complitDate_home=mix_home+"/../dmp-localdata/complitDate/"+target_tb
    if os.path.exists(complitDate_home)==False:
        os.system('mkdir -p '+complitDate_home)
    # 读取完成日期
    complitDate = set()
    if os.path.exists(complitDate_home+"/"+province+"_"+provider+".txt"):
        lines = open(complitDate_home+"/"+province+"_"+provider+".txt","r")
        for line in lines:
            if line>=back_day and line<ARG_OPTIME:
                complitDate.add(line)
        lines.close()
    sys.stdout.flush()
  
    last_day="''" if len(complitDate)==0 else max(complitDate)

    hivesql = []
    hivesql.append('''
        set hive.exec.parallel=true;
        set hive.exec.parallel.thread.number=16;
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert overwrite table %(target_tb)s
        partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,user_type)
        select mix_uid, w1+w2, create_date,last_date,user_type
            from (
            select
                if(a.mix_uid is null, b.mix_uid, a.mix_uid)       as mix_uid,
                if(a.weight is null, '0', a.weight)            as w1,
                if(b.weight is null, '0', b.weight)            as w2,
                if(a.create_date is null,b.day_id,a.create_date)  as create_date,
                if(b.day_id is null, a.last_date, b.day_id)       as last_date,
                if(a.user_type is null,b.user_type,a.user_type)   as user_type
            from (
                select mix_uid,max(weight) as weight,min(create_date) as create_date,max(last_date) as last_date,net_type,user_type
                from %(target_tb)s
                where day_id = %(ARG_OPTIME_DATA_LASTDAY)s and provider = '%(provider)s' 
                      and province = '%(province)s' and net_type='%(NET_TYPE)s'
                 group by mix_uid,net_type,user_type 
                ) a
            full outer join (
                select mix_uid,max(weight) as weight,net_type,day_id,user_type
                from %(source_tb)s
                where day_id = %(ARG_OPTIME)s and provider = '%(provider)s' 
                     and province = '%(province)s' and net_type='%(NET_TYPE)s'
                group by mix_uid, net_type,day_id,user_type
                ) b
            on (a.mix_uid = b.mix_uid  and a.user_type = b.user_type )
            ) c
        where ( case
                when user_type like 'c_%%'
                    then last_date >= %(back_sevenday)s
                else last_date >= %(back_180_day)s end
              )
    ''' % vars())
    HiveExe(hivesql, name, dates)
    
    complitDate.add(ARG_OPTIME)
    # 存储完成日期 
    files = open(complitDate_home+"/"+province+"_"+provider+".txt","w")
    for c in complitDate:
        files.write(c+"\n")
    files.close()
    

    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)