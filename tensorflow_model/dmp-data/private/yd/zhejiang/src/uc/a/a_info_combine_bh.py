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
start_hour_id = sys.argv[3]
end_hour_id = sys.argv[4]
dates = start_hour_id[0:8]
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
    for i in range(int(start_hour_id),int(end_hour_id)+1):
        hivesql.append('''alter table %(source_tb)s add if not exists partition(day_id=%(dates)s,hour_id=%(i)s) location '/aidata/mixdata/srctables/a/dayid=%(dates)s/timeid=%(i)s';''' % vars())
    HiveExe(hivesql,name,dates)
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_dia)s
        (
            phone_no string,
            imei string,
            imsi string,
            out_duration string,
            in_duration string,
            out_times string,
            in_times string,
            out_user_cnt string,
            in_user_cnt string
        )
        partitioned by (provider string,province string,day_id string,hour_id string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_dia)s';
    ''' % vars())
    hivesql.append('''
        create table if not exists %(target_msg)s
        (
            phone_no string,
            imei string,
            imsi string,
            out_num string,
            in_num string,
            out_user_cnt string,
            in_user_cnt string
        )
        partitioned by (provider string,province string,day_id string,hour_id string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_msg)s';
    ''' % vars())
    #执行hivesql语句
    HiveExe(hivesql,name,dates)
    #===========================================================================================
    #插入目标表
    #===========================================================================================
    #过滤锯齿形数据
    hivesql = []
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert overwrite table %(target_dia)s partition(provider='%(provider)s',province='%(province)s',day_id=%(ARG_OPTIME)s,hour_id)
        select case when out.calling is null then in.called else out.calling end,
               case when out.calling_imei is null then in.called_imei else out.calling_imei end,
               case when out.calling_imsi is null then in.called_imsi else out.calling_imsi end,
               case when out.out_duration is null then 0 else out.out_duration end,
               case when in.in_duration is null then 0 else in.in_duration end,
               case when out.out_time is null then 0 else out.out_time end,
               case when in.in_time is null then 0 else in.in_time end,
               case when out.called_num is null then 0 else out.called_num end,
               case when in.calling_num is null then 0 else in.calling_num end,
               out.hour_id as hour_id
        from
            (select calling,calling_imei,calling_imsi,sum(duration) as out_duration,count(distinct called) as called_num,count(*) as out_time,hour_id from(
                select calling,calling_imei,calling_imsi,(unix_timestamp(end_time,'yyyyMMddHHmmss')-unix_timestamp(start_time,'yyyyMMddHHmmss')) as duration,
                called,hour_id from %(source_tb)s where hour_id>=%(start_hour_id)s and hour_id<=%(end_hour_id)s and (cdr_id=0 or cdr_id=1) and event_type=0 and calling != ''
            )aa group by calling,calling_imei,calling_imsi,hour_id) out
            full outer join
            (select called,called_imei,called_imsi,sum(duration) as in_duration,count(distinct calling) as calling_num,count(*) as in_time,hour_id from(
                select called,called_imei,called_imsi,(unix_timestamp(end_time,'yyyyMMddHHmmss')-unix_timestamp(start_time,'yyyyMMddHHmmss')) as duration,
                calling,hour_id from %(source_tb)s where hour_id>=%(start_hour_id)s and hour_id<=%(end_hour_id)s and (cdr_id=0 or cdr_id=1) and event_type=1 and called !=''
            )bb group by called,called_imei,called_imsi,hour_id) in
            on out.calling=in.called and out.hour_id=in.hour_id
    ''' % vars())
    HiveExe(hivesql, name, dates)
    hivesql=[]
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert overwrite table %(target_msg)s partition(provider='%(provider)s',province='%(province)s',day_id=%(ARG_OPTIME)s,hour_id)
        select case when out.calling is null then in.called else out.calling end,
               case when out.calling_imei is null then in.called_imei else out.calling_imei end,
               case when out.calling_imsi is null then in.called_imsi else out.calling_imsi end,
               case when out.out_time is null then 0 else out.out_time end,
               case when in.in_time is null then 0 else in.in_time end,
               case when out.called_num is null then 0 else out.called_num end,
               case when in.calling_num is null then 0 else in.calling_num end,
               out.hour_id as hour_id
        from
            (select calling,calling_imei,calling_imsi,count(distinct called) as called_num,count(*) as out_time,hour_id from(
                select calling,calling_imei,calling_imsi,called,hour_id
                from %(source_tb)s where hour_id>=%(start_hour_id)s and hour_id<=%(end_hour_id)s and (cdr_id=4 or cdr_id=5) and event_type=0 and calling != ''
            )aa group by calling,calling_imei,calling_imsi,hour_id) out
            full outer join
            (select called,called_imei,called_imsi,count(distinct calling) as calling_num,count(*) as in_time,hour_id from(
                select called,called_imei,called_imsi,calling,hour_id
                from %(source_tb)s where hour_id>=%(start_hour_id)s and hour_id<=%(end_hour_id)s and (cdr_id=4 or cdr_id=5) and event_type=1 and called != ''
            )bb group by called,called_imei,called_imsi,hour_id) in
            on out.calling=in.called and out.hour_id=in.hour_id
    ''' % vars())
    HiveExe(hivesql, name, dates)
    #===========================================================================================
    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
