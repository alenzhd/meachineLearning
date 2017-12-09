#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：dmp_lac_ci_center.py
# **  功能描述：获得lac_ci的中心点坐标
# **  输入表：  dmp_uc_otags_m_bh
# **  输出表:   dmp_lac_ci_center
# **  创建者:   zhangqn
# **  创建日期: 2015/08/11
# **  修改日志:
# **  修改日期: 2015/08/21 修改人: zhangqn 修改内容: 改为直接获取GeoHash码
# ** ---------------------------------------------------------------------------------------
# **
# ** ---------------------------------------------------------------------------------------
# **
# **  程序调用格式：python ......py $yyyymmdd jiangsu lt
# **
#********************************************************************************************
# **  Copyright(c) 2015 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
#********************************************************************************************

import sys,os
import datetime
import time
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
    source_tb = "dmp_uc_otags_m_bh"
#===========================================================================================
#自定义变量声明---目标表声明
#===========================================================================================
    target_tb = "dmp_dm_lac_ci_center"
    target_tmp = "dmp_dm_lac_ci_loc_geo_tmp"
#===========================================================================================
#创建创建目标表
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            lac_ci   string,
            loc_center    string,
            radius    double,
            loc_num    int
        )
        partitioned by (provider string,province string,day_id string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())

    hivesql.append('''
        create table if not exists %(target_tmp)s
        (
            mix_m_uid   string,
            lac_ci    string,
            loc_geo    string,
            last_timestamp    string
        )
        partitioned by (provider string,province string,day_id string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tmp)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)
#===========================================================================================
#程序执行
#===========================================================================================
    back_day = get_date_of_back_someday(ARG_OPTIME, 30)
    hour_id = ARG_OPTIME+"00"
    hivesql = []
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.auto.convert.join=false;
        set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

        insert overwrite table %(target_tmp)s
        partition (provider = '%(provider)s',province = '%(province)s',day_id = %(ARG_OPTIME)s)
        select a.mix_m_uid as mix_m_uid,b.tag_index as lac_ci,a.tag_index as loc_geo,a.last_timestamp as last_timestamp
        from (select * from %(source_tb)s where sa_id='loc_geo'
              and day_id>=%(back_day)s and day_id <= %(ARG_OPTIME)s
              and provider = '%(provider)s' and province = '%(province)s') a
        join (select * from %(source_tb)s where sa_id='lac_ci'
              and day_id>=%(back_day)s and day_id <= %(ARG_OPTIME)s
              and provider = '%(provider)s' and province = '%(province)s') b
        on a.mix_m_uid=b.mix_m_uid and a.last_timestamp=b.last_timestamp and a.day_id=b.day_id and a.hour_id=b.hour_id
    ''' % vars())
    HiveExe(hivesql, name, dates)

    hivesql=[]
    hivesql.append('''
        add jar  %(JAR_MIX)s;
        create temporary function DecodeGeoHash as 'com.ai.hive.udf.ud.GeoHashUDF';
        add file %(current_path)s/dmp_lac_ci_center_process.py;
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.auto.convert.join=false;
        set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

        insert overwrite table %(target_tb)s
        partition (provider = '%(provider)s',province = '%(province)s',day_id=%(ARG_OPTIME)s)
        select transform(lac_ci,loc_id_set)
        using 'python dmp_lac_ci_center_process.py'
        as (lac_ci string,loc_center string,radius double,loc_num int)
        from (
            select lac_ci,concat_ws('|',collect_set(concat(loc_id,',',loc_geo))) as loc_id_set,count(*) as cnt
            from (
                select lac_ci,loc_geo,DecodeGeoHash(loc_geo) as loc_id
                from %(target_tmp)s
                where day_id= %(ARG_OPTIME)s
            ) b
        group by lac_ci
        ) c
    ''' % vars())
    HiveExe(hivesql, name, dates)
    
    hivesql = []
    hivesql.append('''
    alter table %(target_tmp)s  drop partition(provider = '%(provider)s',province = '%(province)s',day_id=%(ARG_OPTIME)s);
    ''' % vars())

    HiveExe(hivesql,name,dates)
    #===========================================================================================

    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
