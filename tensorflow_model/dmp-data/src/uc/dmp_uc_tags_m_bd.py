#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称： xxx.py
# **  功能描述： 江苏联通关键词标准化
# **  输入表：   dmp_uc_tags_m_bh 
# **  输出表:    dmp_uc_tags_m_bd
# **  创建者:    luozs
# **  创建日期:  2016/11/07
# **  修改日志:
# **  修改日期: yyyy/mm/dd，修改人 xxx  ，修改内容：
# ** ---------------------------------------------------------------------------------------
# **  程序调用格式：python ......py yyyymmdd(hh)

import sys,os
import datetime
import time
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
    source_tb_uctags = "dmp_uc_tags_m_bh"
#===========================================================================================
#自定义变量声明---目标表声明
#===========================================================================================
    target_tb = "dmp_uc_tags_m_bd"    
#===========================================================================================
#创建创建目标表
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
		    mix_m_uid string,
			app_id string,
			site_id string,
			cont_id string,
			action_id string,
			value string,
			value_type_id string,
			duration string,
			last_timestamp string,
			count int
        )
        partitioned by (provider string,province string,day_id string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)

#================================================================================
#将开头为L的cont_id转换为数字的cont_id
#set hive.exec.dynamic.partition=true;
#set hive.exec.dynamic.partition.mode=nonstrict;
#================================================================================
    hivesql = []
    hivesql.append('''
    add file %(current_path)s/juchi.py;
    insert overwrite table %(target_tb)s
    partition (provider = '%(provider)s',province = '%(province)s',day_id=%(ARG_OPTIME)s)
    select transform(mix_m_uid,app_id,site_id,cont_id,action_id,value,value_type_id,duration,last_timestamp,count)
    using 'python juchi.py'
    as (mix_m_uid,app_id,site_id,cont_id,action_id,value,value_type_id,duration,last_timestamp,count)
  
    from (
	  select * from 
      %(source_tb_uctags)s 
      where provider = '%(provider)s' and province = '%(province)s' and day_id=%(ARG_OPTIME)s
      and value_type_id = 1 and length(value) >1
	  distribute by mix_m_uid
	  sort by mix_m_uid,last_timestamp asc
	) t1
    
	
    ''' % vars())
    HiveExe(hivesql, name, dates)
    
#程序结束
    End(name,dates)

#异常处理
except Exception,e:
    Except(name,dates,e)