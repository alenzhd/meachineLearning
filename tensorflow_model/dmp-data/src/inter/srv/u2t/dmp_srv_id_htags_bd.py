#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：xxxxxxxxx
# **  输入表：  xxxxxxxxx
# **  输出表:   xxxxxxxxx
# **  创建者:   xxxxxxxxx
# **  创建日期: yyyy/mm/dd
# **  修改日志:
# **  修改日期: yyyy/mm/dd，修改人 xxx  ，修改内容：流程简化
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
#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../../../'
python_path = []
python_path.append(mix_home+'conf')
sys.path[:0]=python_path
print sys.path

#引入自定义包
from settings import *

#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
dates = sys.argv[4]

try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    ARG_OPTIME = dicts['ARG_OPTIME']
    ARG_OPTIME_LASTDAY = dicts['ARG_OPTIME_LASTDAY']

#===========================================================================================
#自定义变量声明---源表声明
#===========================================================================================
    source_tb = "dmp_ud_id_tags_bd"
    ipinyou_deviceid_tb = "dmp_cop.dmp_cop_ipinyou_device_dt"
#===========================================================================================
#自定义变量声明---目标表声明
#===========================================================================================
    target_tb = "dmp_srv_id_htags_bd"
#===========================================================================================
#获取最近的时间
#===========================================================================================
    filepath = '/user/hive/warehouse/dmptest_user_dir/dmp_cop/dmp_cop_ipinyou_device_dt'
    print filepath
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
            key   string,
            value    string
        )
        partitioned by (provider string,province string,net_type string,day_id string,flag string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)
#===========================================================================================
#程序执行
#===========================================================================================
    hivesql=[]
    hivesql.append('add file ' + current_path + "/trans_ual.py")
    hivesql.append('add jar '+JAR_MIX)
    hivesql.append('''
        set hive.exec.compress.output=false;
        set mapreduce.map.output.compress=false;
        set mapreduce.output.fileoutputformat.compress=false;
        set mapreduce.job.reduces=60;
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert overwrite table %(target_tb)s 
        partition (provider='%(PROVIDER)s',province='%(PROVINCE)s',net_type='%(NET_TYPE)s',day_id='%(ARG_OPTIME)s',flag)
            select key,value,flag from ( 
            select transform(flag,flag_id,day_id,type_id,tags,weight) 
                   using 'python trans_ual.py' 
                   as (key,value,flag)
            from (
                   select flag, flag_id, day_id, type_id, tags, weight
                        from %(source_tb)s
                        where provider='%(PROVIDER)s' and province='%(PROVINCE)s' and net_type='%(NET_TYPE)s' and day_id='%(ARG_OPTIME)s'
                            and flag in ('ip', 'c_ipinyou', 'imsi')
                        cluster by flag,flag_id
                   union all
                   select 'idfa' as flag, a.flag_id, a.day_id, a.type_id, a.tags, a.weight
                        from(
                            select flag_id,day_id,type_id,tags,weight
                                from %(source_tb)s
                                where provider='%(PROVIDER)s' and province='%(PROVINCE)s' and net_type='%(NET_TYPE)s' and day_id='%(ARG_OPTIME)s'
                                    and flag = 'idfa'
                        )a
                        join (
                            select device_type, device_id
                                from %(ipinyou_deviceid_tb)s
                                where device_type = 'idfa' and day_id='%(ARG_OPTIME_DATA_LASTDAY)s'
                        )b
                        on (a.flag_id = b.device_id)
                        cluster by flag,a.flag_id
                   union all
                   select 'imei' as flag, aa.flag_id, aa.day_id, aa.type_id, aa.tags, aa.weight
                        from(
                            select flag_id,day_id,type_id,tags,weight
                                from %(source_tb)s
                                where provider='%(PROVIDER)s' and province='%(PROVINCE)s' and net_type='%(NET_TYPE)s' and day_id='%(ARG_OPTIME)s'
                                    and flag = 'md532_imei'
                        )aa
                        join (
                            select device_type, device_id
                                from %(ipinyou_deviceid_tb)s
                                where device_type = 'md532_imei' and day_id='%(ARG_OPTIME_DATA_LASTDAY)s'
                        )bb
                        on (aa.flag_id = bb.device_id)
                        cluster by flag,aa.flag_id
                 ) t1
            ) t2
        
    ''' % vars())
    HiveExe(hivesql, name, dates)

    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
