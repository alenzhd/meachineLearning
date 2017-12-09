#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：dmp_um_id_rel_bd.py
# **  功能描述：用户关系天表
# **  输入表： dmp_um_id_rel_bd, (dmp_um_all_imei_rel_dt)
# **  输出表:  dmp_um_id_rel_bd
# **  创建者:  hulx
# **  创建日期: 2016/07/27
# **  修改日志:
# **  修改日期: yyyy/mm/dd，修改人 xxx  ，修改内容：
# ** ---------------------------------------------------------------------------------------
# **
# ** ---------------------------------------------------------------------------------------
# **
# **  程序调用格式：python ......py yyyymmdd(hh)
# **
#********************************************************************************************
# **  Copyright(c) 2015 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
#********************************************************************************************


import sys,os, subprocess
import datetime
import time
import xml.dom.minidom


#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../'
python_path = []
python_path.append(mix_home+'conf')
sys.path[:0]=python_path
print sys.path

#引入自定义包
from settings import *

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
    source_tb = "dmp_um_all_imei_rel_dt"
    #===========================================================================================
    #自定义变量声明---目标表声明
    #===========================================================================================
    target_tb = "dmp_um_id_rel_bd"

    filepath = HIVE_TB_HOME + '/' + source_tb + '/provider='+provider+'/province='+province
    print filepath
    cat = subprocess.Popen(['hadoop', 'fs', '-du',filepath],stdout=subprocess.PIPE)
    res = ARG_OPTIME_LASTDAY
    for line in cat.stdout:
        if 'day_id=' in line and int(line.split(' ')[0].strip()) >= 1048576:
            tmp = line.split('=')[-1].strip()
            if int(tmp) < int(ARG_OPTIME):
                res = tmp
    ARG_OPTIME_DATA_LASTDAY = res.strip()
    #===============================================================================
    #电信云dxy
    #imei为明文，s_imei/s_meid为密文
    #imei存到flag = std_imei
    #将 s_imei 与dmp_um_all_imei_rel_dt表的14位加密imei匹配出明文imei，存到flag = std_imei。
    #===============================================================================
    if provider == 'dxy' and NET_TYPE == 'mobile':
        hivesql = []
        hivesql.append('''
            set hive.exec.dynamic.partition=true;
            set hive.exec.dynamic.partition.mode=nonstrict;
            set hive.optimize.sort.dynamic.partition=false;
            set mapreduce.map.memory.mb=2048;
            set mapreduce.reduce.memory.mb=4096;
            add file %(current_path)s/merge_hour_list.py;
            insert overwrite table %(target_tb)s
                partition(provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',
                        day_id=%(ARG_OPTIME)s,user_type = 'mobile',flag = 'std_imei')
            select transform(mix_uid, flag_id, weight, hour_list)
                using 'python merge_hour_list.py'
                as (mix_uid, flag_id, weight, hour_list)
            from(
                select mix_uid,flag_id,weight,hour_list
                from (
                    select mix_uid, flag_id, weight,hour_list
                    from %(target_tb)s
                    where provider='dxy' and province='%(province)s' and day_id=%(ARG_OPTIME)s
                        and net_type='%(NET_TYPE)s' and user_type = 'mobile' and flag ='imei'
                    UNION ALL
                    select a.mix_uid, b.imei as flag_id, a.weight, a.hour_list
                    from(
                        select transform(mix_uid, flag_id, weight, hour_list)
                            using 'python merge_hour_list.py'
                            as (mix_uid, flag_id, weight, hour_list)
                            from(
                             select mix_uid,flag_id,weight,hour_list
                                from %(target_tb)s
                                where provider='dxy' and province='%(province)s' and day_id=%(ARG_OPTIME)s
                                    and net_type='%(NET_TYPE)s' and user_type = 'mobile' and flag in ('s_imei', 's_meid')
                                cluster by mix_uid,flag_id
                            )t
                    )a
                    join(
                        select imei, imei_e
                        from %(source_tb)s
                        where provider = 'dxy' and province='%(province)s' and day_id = %(ARG_OPTIME_DATA_LASTDAY)s
                            and encrypt_type = '14md532'
                    )b
                    on (a.flag_id = b.imei_e)
                )t1
                cluster by mix_uid, flag_id
            )t2

        ''' % vars())
        HiveExe(hivesql, name, dates)

    #===============================================================================
    #江苏联通jslt
    #imei为明文，s_imei为明文
    #imei和s_imei存到flag = std_imei
    #===============================================================================
    if provider == 'lt' and NET_TYPE == 'mobile':
        hivesql = []
        hivesql.append('''
            set hive.exec.dynamic.partition=true;
            set hive.exec.dynamic.partition.mode=nonstrict;
            set hive.optimize.sort.dynamic.partition=false;
            set mapreduce.map.memory.mb=2048;
            set mapreduce.reduce.memory.mb=4096;
            add file %(current_path)s/merge_hour_list.py;
            insert overwrite table %(target_tb)s
                partition(provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',
                        day_id=%(ARG_OPTIME)s,user_type = 'mobile',flag = 'std_imei')
            select transform(mix_uid, flag_id, weight, hour_list)
                using 'python merge_hour_list.py'
                as (mix_uid, flag_id, weight, hour_list)
                from(
                    select mix_uid, flag_id, weight,hour_list
                        from %(target_tb)s
                        where provider='lt' and province='%(province)s' and day_id=%(ARG_OPTIME)s
                        and net_type='%(NET_TYPE)s' and user_type = 'mobile' and flag in ('imei','s_imei')
                        cluster by mix_uid,flag_id
            )t
        ''' % vars())
        HiveExe(hivesql, name, dates)

#程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)