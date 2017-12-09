#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：xxxxxxxxx
# **  输入表：  dmp_ci_bh
# **  输出表:   dmp_table
# **  创建者:   xxx
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
import os,sys
#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../../../../../'
python_path = []
python_path.append(mix_home+'/conf')
sys.path[:0]=python_path

#引入自定义包
from settings import *

#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
dates = sys.argv[3]
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
    source_tb = "src_a_data"
    #===========================================================================================
    #自定义变量声明---目标表声明
    target_dia = "dmp_uc_a_dialogue_m_bh"
    target_msg = "dmp_uc_a_message_m_bh"
    target_loc = "dmp_uc_a_location_m_bd"
    target_rel = "dmp_uc_a_users_relation_m_bd"
    #===========================================================================================
    #创建创建目标表
    #===========================================================================================
    hivesql = []
    for i in range(int(dates+"00"),int(dates+"23")+1):
        hivesql.append('''alter table %(source_tb)s add if not exists partition(day_id=%(dates)s,hour_id=%(i)s) location '/aidata/mixdata/srctables/a/dayid=%(dates)s/timeid=%(i)s';''' % vars())
    #执行hivesql语句
    HiveExe(hivesql,name,dates)

    hivesql = []
    hivesql.append('''
        create table if not exists %(target_loc)s
        (
            phone_no string,
            imei string,
            imsi string,
            loc string,
            date string
        )
        partitioned by (provider string,province string,day_id string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_loc)s';
    ''' % vars())
    hivesql.append('''
        create table if not exists %(target_rel)s
        (
            phone_no string,
            imei string,
            imsi string,
            in_phone_no string,
            in_imei string,
            in_imsi string,
            start_time string,
            duration string,
            location string,
            type string
        )
        partitioned by (provider string,province string,day_id string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_rel)s';
    ''' % vars())
    #执行hivesql语句
    HiveExe(hivesql,name,dates)
    #===========================================================================================
    #插入目标表
    #===========================================================================================
    #过滤锯齿形数据
    hivesql = []
    hivesql.append('''
        insert overwrite table %(target_loc)s partition(provider='%(provider)s',province='%(province)s',day_id=%(ARG_OPTIME)s)
        select distinct calling_all,calling_imei,calling_imsi,concat_ws('_',lac_id,cell_id) as loc,end_time from
            (select calling as calling_all,calling_imei,calling_imsi,current_lac as lac_id,current_ci_sac as cell_id,end_time
            FROM %(source_tb)s 
            WHERE (cdr_id='2' or cdr_id='3') and day_id=%(ARG_OPTIME)s
            UNION ALL
            select CASE WHEN event_type='1' or event_type='6' then called ELSE calling END as calling_all,calling_imei,calling_imsi,
            case when cdr_id='0' then first_lac else current_lac end as lac_id,
            case when cdr_id='0' then first_ci_sac else current_ci_sac end as cell_id,end_time
            FROM %(source_tb)s 
            WHERE (cdr_id ='0' or cdr_id ='1') and event_type!='2' and event_type!='3' and event_type!='4' and event_type!='7' and event_type!='8' and day_id=%(ARG_OPTIME)s
            UNION ALL
            select case when event_type='1' then called else calling end as calling_all,calling_imei,calling_imsi,first_lac as lac_id,first_ci_sac as cell_id,end_time
            FROM %(source_tb)s 
            WHERE (cdr_id ='4' or cdr_id ='5') and day_id=%(ARG_OPTIME)s) t 
        where t.calling_all!=''
    ''' % vars())
    HiveExe(hivesql, name, dates)
    hivesql=[]
    hivesql.append('''
        insert overwrite table %(target_rel)s partition(provider='%(provider)s',province='%(province)s',day_id=%(ARG_OPTIME)s)
        select calling,calling_imei,calling_imsi,called,called_imei,called_imsi,start_time,
        (unix_timestamp(end_time,'yyyyMMddHHmmss')-unix_timestamp(start_time,'yyyyMMddHHmmss')) as duration,
        case when (cdr_id=0 or cdr_id=1) then concat_ws('_',current_lac,current_ci_sac)
             when (cdr_id=4 or cdr_id=5) then first_lac else '' end,
        case when (cdr_id=0 or cdr_id=1) then 'call'
             when (cdr_id=4 or cdr_id=5) then 'message' else '' end
        from %(source_tb)s where day_id=%(ARG_OPTIME)s and (cdr_id=0 or cdr_id=1 or cdr_id=4 or cdr_id=5) and called != ''
    ''' % vars())
    HiveExe(hivesql, name, dates)
    #===========================================================================================
    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
