#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称： dmp_um_adx_cm_rel_bd.py
# **  功能描述：用户关系累计表
# **  输入表： dmp_um_adx_cm_rel_bd_tmp
# **  输出表:  dmp_um_adx_cm_rel_bd
# **  创建者:  xiezh
# **  创建日期: 2016/04/24
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

import sys,os
import datetime
import time
import xml.dom.minidom

#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../../'
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
    #===========================================================================================
    #自定义变量声明---源表声明
    #===========================================================================================
    source_tb = "dmp.dmp_um_adx_cm_rel_bd_tmp"
    #===========================================================================================
    #自定义变量声明---目标表声明
    #===========================================================================================
    target_tb = "dmp_um_adx_cm_rel_dt"
    #===========================================================================================
    #创建创建目标表
    #===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            adx_cookie_id string,
            adx_uid string
        )
        PARTITIONED BY(provider string,province string,day_id string,dsp_type string,adx_type string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)
#===========================================================================================
#累计
#===========================================================================================
    back_day = get_date_of_back_someday(ARG_OPTIME, 15)
    hivesql = []
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table %(target_tb)s 
        partition(provider = '%(PROVIDER)s',province='%(PROVINCE)s',day_id='%(ARG_OPTIME)s',dsp_type,adx_type) 
	select adx_cookie_id,adx_uid,dsp_type,adx_type from %(source_tb)s 
        where provider = '%(PROVIDER)s' and province='%(PROVINCE)s' and day_id >= '%(back_day)s' 
        
    ''' % vars())
    HiveExe(hivesql, name, dates)

    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
