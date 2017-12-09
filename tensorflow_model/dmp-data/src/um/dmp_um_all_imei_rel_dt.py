#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：dmp_um_all_imei_rel_dt.py
# **  功能描述：表
# **  输入表： dmp_um_all_imei_rel_dt
# **  输出表:  dim_um_all_imei,dmp_um_id_rel_dt
# **  创建者:  hulx
# **  创建日期: 2016/07/20
# **  修改日志:
# **  修改日期: yyyy/mm/dd，修改人 xxx  ，修改内容：
# ** ---------------------------------------------------------------------------------------
# **
# ** ---------------------------------------------------------------------------------------
# **
# **  程序调用格式：
# **
#********************************************************************************************
# **  Copyright(c) 2015 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
#********************************************************************************************


import sys,os
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
    source_imei_tb = "dim_um_all_imei"
    source_id_rel_tb = "dmp_um_id_rel_dt"
#===========================================================================================
#自定义变量声明---目标表声明
#===========================================================================================
    target_tb = "dmp_um_all_imei_rel_dt"

#===========================================================================================
#创建原始表source_imei_tb
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(source_imei_tb)s
        (
            imei string,
            distance string
        )
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)

#===========================================================================================
#创建目标表dmp_um_all_imei_rel_dt
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            imei string,
            imei_e string
        )
        partitioned by (provider string, province string, day_id string, encrypt_type string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)

#===========================================================================================
#生成目标表14md532分区
#===========================================================================================
    hivesql = []
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        add jar %(JAR_MIX)s ;
        create temporary function StdMd5ImeiUDF as 'com.ai.hive.udf.util.StdMd5ImeiUDF';
        create temporary function ImeiStandardizationUDF as 'com.ai.hive.udf.srvutil.ImeiStandardizationUDF';
        insert overwrite table %(target_tb)s
        partition (provider, province, day_id, encrypt_type)
        select a.imei, a.md5imei, b.provider, b.province, b.day_id, '14md532'
        from(
            select ImeiStandardizationUDF(imei) as imei, StdMd5ImeiUDF(imei) as md5imei
            from %(source_imei_tb)s
        )a
        join (
            select flag_id, provider, province, day_id
            from %(source_id_rel_tb)s
            where provider = 'dxy' and user_type = 'mobile' and flag = 's_imei' and day_id = %(ARG_OPTIME)s
        )b
        on(a.md5imei = b.flag_id)
        group by a.imei, a.md5imei, b.provider, b.province, b.day_id
    '''% vars())
    HiveExe(hivesql, name, dates)

#===========================================================================================
# (暂时未用！！)
# 生成目标表md532分区
#===========================================================================================
    hivesql = []
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        add jar %(JAR_MIX)s ;
        create temporary function Md5UDF as 'com.ai.hive.udf.util.Md5UDF';
        insert overwrite table %(target_tb)s
        partition (provider, province, day_id, encrypt_type)
        select imei, Md5UDF(imei), provider, province, day_id, 'md532'
        from %(target_tb)s
        where day_id = %(ARG_OPTIME)s

    '''% vars())
    # HiveExe(hivesql, name, dates)

#程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)