#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称： dmp_um_dspcookie_adx_rel_bd.py
# **  功能描述：用户关系累计表
# **  输入表： dim_geo_pinyou、ipinyou_user_pc
# **  输出表:  dmp_um_dspcookie_adx_rel_bd
# **  创建者:  guojy
# **  创建日期: 2016/03/10
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
    source_tb1 = "dmpbak.dim_geo_ipinyou"
    source_tb2 = "dmpbak.ipinyou_user_pc"
    #===========================================================================================
    #自定义变量声明---目标表声明
    #===========================================================================================
    target_tb1 = "dmp_um_dspcookie_adx_rel_bd"
    target_tb2 = "dmp_um_dspcookie_adx_rel_dt"
    #===========================================================================================
    #创建创建目标表1
    #===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb1)s
        (
            dsp_cookie_id string,
            adx_uid string
        )
        PARTITIONED BY(province string,day_id string,dsp_type string,adx_type string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb1)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)
    #===========================================================================================
    #创建创建目标表2
    #===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb2)s
        (
            dsp_cookie_id string,
            adx_uid string
        )
        PARTITIONED BY(province string,day_id string,dsp_type string,adx_type string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb2)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)
    #===========================================================================================
    #解析C类数据ipinyou_user_pc
    #===========================================================================================
    hivesql = []
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table %(target_tb1)s 
        partition(province,day_id,dsp_type,adx_type)
        select /*+mapjoin(a)*/ 
		b.pyid,
		b.tid,
		a.parent_name_en,
		'%(ARG_OPTIME)s',
		'ipinyou',
		b.pl 
        from (select * from %(source_tb1)s where id < 400 ) a join (select * from %(source_tb2)s where day_id='%(ARG_OPTIME)s') b 
        on a.id=b.geo group by 
		b.pyid,
		b.tid,
		a.parent_name_en,
		'%(ARG_OPTIME)s',
		'ipinyou',
		b.pl 
    ''' % vars())
    HiveExe(hivesql, name, dates)
    #===========================================================================================
    #累计15天
    #===========================================================================================
    back_day = get_date_of_back_someday(ARG_OPTIME, 15)
    hivesql = []
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table %(target_tb2)s 
        partition(province,day_id,dsp_type,adx_type)
        select dsp_cookie_id,adx_uid,province,'%(ARG_OPTIME)s',dsp_type,adx_type from %(target_tb1)s where province in ('shanghai','jiangsu','zhejiang','anhui','fujian','shandong','guangdong','chongqing','sichuan','hainan') and day_id >= '%(back_day)s' 
        group by dsp_cookie_id,adx_uid,province,'%(ARG_OPTIME)s',dsp_type,adx_type 
    ''' % vars())
    HiveExe(hivesql, name, dates)

    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
