#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：xxxxxxxxx
# **  输入表：  dmp_ci_bh
# **  输出表:   dmp_table
# **  创建者:   fengbo
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
from xml.dom.minidom import parse
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

#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
dates = sys.argv[3]
province = PROVINCE
provider = PROVIDER
try:
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    ARG_OPTIME = dicts['ARG_OPTIME']

    sta_tb_tmp = "dmp_ud_ul_statistic_tmp"
    sta_tb = "dmp_mn_kpi_bd"
    statistic_path = HIVE_TB_HOME+"/ul/statistic"
    
    #上层标签统计数据
    hivesql=[]
    hivesql.append('''
        create EXTERNAL table if not exists %(sta_tb_tmp)s (
            key string,
            value string
        )
        partitioned by (type string,provider string,province string,net_type string,day_id string)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
        LOCATION '%(statistic_path)s';
    ''' % vars())
    hivesql.append('''
        alter table %(sta_tb_tmp)s add if not exists partition(type='statistic',provider='%(provider)s',province='%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s) location 'statistic/%(provider)s/%(province)s/%(NET_TYPE)s/%(ARG_OPTIME)s';
        alter table %(sta_tb_tmp)s add if not exists partition(type='demo',provider='%(provider)s',province='%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s) location 'demo/%(provider)s/%(province)s/%(NET_TYPE)s/%(ARG_OPTIME)s';
        alter table %(sta_tb_tmp)s add if not exists partition(type='summary',provider='%(provider)s',province='%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s) location 'summary/%(provider)s/%(province)s/%(NET_TYPE)s/%(ARG_OPTIME)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)
    hivesql=[]
    hivesql.append('''
        insert overwrite table %(sta_tb)s partition(provider='%(provider)s',province='%(province)s',net_type='mobile',day_id=%(ARG_OPTIME)s,module='statistic')
        select concat_ws('|','ud',concat_ws('_',type,key),'','','%(ARG_OPTIME)s','%(province)s','%(provider)s','%(NET_TYPE)s','click','','day'),value
        from %(sta_tb_tmp)s where day_id=%(ARG_OPTIME)s
    ''' % vars())
    HiveExe(hivesql, name, dates)
    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)