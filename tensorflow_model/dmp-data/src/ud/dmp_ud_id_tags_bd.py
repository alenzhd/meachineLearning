#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：天级ID标签表，存储每天ID的所有标签（需去重）
# **  输入表：  dmp_ud_user_tags_bd 、dmp_um_user_rel_dt
# **  输出表:   dmp_ud_id_tags_bd
# **  创建者:   guojy
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
# **  Copyright(c) 2013 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
#********************************************************************************************

import sys,os
import datetime
import time
import subprocess

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
    source_tb_uid = "dmp_ud_user_stdtags_bd"
    source_tb_rel = "dmp_um_user_rel_dt"
    source_tb_cookie_rel = "dmp_um_cookie_rel_dt"
    source_tb_cont_l_rel = DMP_CONSOLE + ".dim_cont_l_rel"
#===========================================================================================
#自定义变量声明---目标表声明
#===========================================================================================
    target_tb = "dmp_ud_id_tags_bd"
#===========================================================================================
#获取最近一天的日期 dmp_um_user_rel_dt
#===========================================================================================
    filepath = HIVE_TB_HOME + '/dmp_um_user_rel_dt/provider='+provider+'/province='+province+'/net_type='+NET_TYPE
    cat = subprocess.Popen(['hadoop', 'fs', '-du',filepath],stdout=subprocess.PIPE)
    res = ARG_OPTIME_LASTDAY
    for line in cat.stdout:
        if 'day_id=' in line and int(line.split(' ')[0].strip()) >= 1048576:
            tmp = line.split('=')[-1].strip()
            if int(tmp) <= int(ARG_OPTIME):
                res = tmp
    ARG_OPTIME_DATA_LASTDAY = res.strip()
#===========================================================================================
#获取最近一天的日期 dmp_um_user_dt
#===========================================================================================
    filepath = HIVE_TB_HOME + '/dmp_um_user_dt/provider='+provider+'/province='+province+'/net_type='+NET_TYPE
    cat = subprocess.Popen(['hadoop', 'fs', '-du',filepath],stdout=subprocess.PIPE)
    res = ARG_OPTIME_LASTDAY
    for line in cat.stdout:
        if 'day_id=' in line and int(line.split(' ')[0].strip()) >= 1048576:
            tmp = line.split('=')[-1].strip()
            if int(tmp) <= int(ARG_OPTIME):
                res = tmp
    ARG_OPTIME_DATA_LASTDAY_user_dt = res.strip()
#===========================================================================================
#创建创建目标表
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            flag_id   string,
            type_id   string,
            tags      string,
            weight    double
        )
        partitioned by (provider string,province string,net_type string,day_id string,flag string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)

#================================================================================
#将f标签分解为0和b标签
#================================================================================
    hivesql = []
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        add file %(current_path)s/dmp_parse_ftags.py;
        insert into table %(source_tb_uid)s
        partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s, user_type)
        select t3.mix_uid, t3.type_id, t3.tags, t3.weight, t3.hour_list, t3.user_type
        from
        (   select transform(mix_uid,type_id,tags,weight,hour_list,user_type)
            using 'python dmp_parse_ftags.py'
            as (mix_uid, type_id, tags, weight, hour_list,user_type)
            from %(source_tb_uid)s
            where type_id ='f' and day_id =%(ARG_OPTIME)s and provider = '%(provider)s'
                  and province = '%(province)s' and net_type='%(NET_TYPE)s'
        ) t3
        ''' % vars())
    #HiveExe(hivesql, name, dates)

#================================================================================
#将开头为L的cont_id转换为数字的cont_id
#================================================================================
    hivesql = []
    hivesql.append('''
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    add file %(current_path)s/merge_tags_hour_list.py;
    insert overwrite table %(source_tb_uid)s
    partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s, user_type)
    select transform(mix_uid, type_id, tags, hour_list,user_type)
    using 'python merge_tags_hour_list.py'
    as (mix_uid, type_id, tags, weight, hour_list,user_type)
    from(
        select mix_uid, type_id, tags, user_type, hour_list
        from (
            select mix_uid, type_id, tags, weight, hour_list, user_type
                from %(source_tb_uid)s
                where (type_id !='0' or tags not like 'L%%') and day_id =%(ARG_OPTIME)s and provider = '%(provider)s'
                      and province = '%(province)s' and net_type='%(NET_TYPE)s'
            union all
            select mix_uid, type_id , b.cont_id as tags, weight, hour_list, user_type
                from(
                     select mix_uid, type_id , tags, weight, hour_list, user_type
                         from %(source_tb_uid)s
                         where tags like 'L%%' and type_id = '0' and day_id =%(ARG_OPTIME)s and provider = '%(provider)s'
                         and province = '%(province)s' and net_type='%(NET_TYPE)s'
                )a
                join (
                     select l_cont_id, cont_id
                         from %(source_tb_cont_l_rel)s
                )b
                on (a.tags = b.l_cont_id)
                group by mix_uid, type_id, b.cont_id, weight, hour_list, user_type
        )t1
        cluster by user_type, mix_uid, type_id, tags, hour_list
    )t2
    group by mix_uid, type_id, tags, hour_list, user_type
    ''' % vars())
    #HiveExe(hivesql, name, dates)

#===========================================================================================
#生成表,可通过con配置表
#===========================================================================================
    if NET_TYPE == 'adsl':
        con = "net_type='adsl' and flag in ('ip')"
        con1 = "net_type = 'adsl' and user_type = 'ad'"
    elif NET_TYPE == 'mobile':
        con = "net_type = 'mobile' and flag in ('idfa', 'imsi', 'md532_imei', 'std_imei')"
        con1 = "net_type = 'mobile' and user_type = 'mobile'"
    hivesql = []
    hivesql.append('''
    set hive.auto.convert.join=false;
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    add file %(current_path)s/idmapping_to_tags.py;
    insert overwrite table %(target_tb)s
    partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,flag)
    select TRANSFORM(t.type_id,t.tags,t.weight,t.mix_uid,t.flag,t.cweight,t.flag_id) USING 'python idmapping_to_tags.py' AS (flag_id,type_id,tags,weight,flag)
    from(
        select a.type_id, a.tags, a.weight, c.mix_uid, b.flag , c.weight as cweight, b.flag_id
        from (select mix_uid,type_id,tags,weight,day_id,user_type
                  from %(source_tb_uid)s
                  where day_id =%(ARG_OPTIME)s and provider = '%(provider)s'
                  and province = '%(province)s' and %(con1)s
        ) a
        join (select mix_uid,flag_id,user_type,flag
                  from %(source_tb_rel)s
                  where day_id =%(ARG_OPTIME_DATA_LASTDAY)s and provider = '%(provider)s' and province = '%(province)s'
                  and %(con)s
        ) b
        on (a.mix_uid = b.mix_uid  and a.user_type = b.user_type)
        join (select mix_uid, weight, user_type
              from dmp_um_user_dt
              where day_id =%(ARG_OPTIME_DATA_LASTDAY_user_dt)s and provider = '%(provider)s' and province = '%(province)s'
              and %(con1)s
        ) c
        on (a.mix_uid = c.mix_uid and a.user_type = c.user_type)
        cluster by b.flag_id
    )t
    ''' % vars())
    HiveExe(hivesql, name, dates)
    
#======================================================================
#固网把imei、idfa用户的标签打到flag=std_imei\md532_imei\idfa
#======================================================================
    if NET_TYPE == 'adsl':
        hivesql = []
        hivesql.append('''
        set hive.auto.convert.join=false;
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=false;
        insert overwrite table %(target_tb)s
        partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,flag)
        select mix_uid,type_id,tags,weight,'std_imei'
              from %(source_tb_uid)s
              where day_id =%(ARG_OPTIME)s and provider = '%(provider)s'
              and province = '%(province)s' and net_type = 'adsl' and user_type = 'imei'
        ''' % vars())
        HiveExe(hivesql, name, dates)

        hivesql = []
        hivesql.append('''
        set hive.auto.convert.join=false;
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=false;
        set mapreduce.map.memory.mb=2048;
        set mapreduce.reduce.memory.mb=4096;
        add jar %(JAR_MIX)s ;
        create temporary function Md5UDF as 'com.ai.hive.udf.util.Md5UDF';
        insert overwrite table %(target_tb)s
        partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,flag)
        select Md5UDF(mix_uid),type_id,tags,weight,'md532_imei'
              from %(source_tb_uid)s
              where day_id =%(ARG_OPTIME)s and provider = '%(provider)s'
              and province = '%(province)s' and net_type = 'adsl' and user_type = 'imei'
        ''' % vars())
        HiveExe(hivesql, name, dates)

        hivesql = []
        hivesql.append('''
        set hive.auto.convert.join=false;
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=false;
        insert overwrite table %(target_tb)s
        partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,flag)
        select mix_uid,type_id,tags,weight,'idfa'
              from %(source_tb_uid)s
              where day_id =%(ARG_OPTIME)s and provider = '%(provider)s'
              and province = '%(province)s' and net_type = 'adsl' and user_type = 'idfa'
        ''' % vars())
        HiveExe(hivesql, name, dates)


#======================================================================
#固网flag加入'c_ipinyou', 'c_ali', 'c_baidu'
#======================================================================
    if NET_TYPE == 'adsl':
        hivesql = []
        hivesql.append('''
            set hive.exec.dynamic.partition=true;
            set hive.exec.dynamic.partition.mode=nonstrict;
            insert overwrite table %(target_tb)s
            partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,flag)
            select b.flag_id,a.type_id,a.tags,a.weight,a.user_type
            from (select mix_uid,type_id,tags,weight,day_id,user_type
                  from %(source_tb_uid)s
                  where day_id =%(ARG_OPTIME)s and provider = '%(provider)s' 
                      and province = '%(province)s' and net_type='%(NET_TYPE)s'
                      and user_type in ('c_ipinyou', 'c_ali', 'c_baidu')
                  ) a
            join (select mix_uid,flag_id,user_type, flag
                  from %(source_tb_rel)s
                  where day_id =%(ARG_OPTIME_DATA_LASTDAY)s and provider = '%(provider)s' and province = '%(province)s'
                      and net_type='%(NET_TYPE)s' and user_type in ('c_ipinyou', 'c_ali', 'c_baidu') 
                      and (flag = user_type or flag = 'cproid')
                  ) b
            on (a.mix_uid = b.mix_uid and a.user_type = b.user_type)
            group by b.flag_id,a.type_id,a.tags,a.weight,a.user_type
        ''' % vars())
        HiveExe(hivesql, name, dates)

#================================================================================
#通过cookiemapping 把flag = c_ali、flag = c_baidu、flag = c_ipinyou 的标签加入到flag= c_ipinyou
#================================================================================
        hivesql = []
        hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        add file %(current_path)s/cookiemapping_to_tags.py;
        insert overwrite table %(target_tb)s
        partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,flag)
        select TRANSFORM(flag_id,type_id,tags,weight) using 'python cookiemapping_to_tags.py' as (flag_id,type_id,tags,weight,flag)
        from (
               select flag_id,type_id,tags,weight
               from (
                     select b.dsp_cookie_id as flag_id, a.type_id as type_id, a.tags as tags,a.weight as weight
                     from(
                          select flag_id, type_id, tags, weight,flag
                          from %(target_tb)s
                          where day_id =%(ARG_OPTIME)s and provider = '%(provider)s' 
                               and province = '%(province)s' and flag in ('c_baidu', 'c_ali')
                         ) a 
                     join(
                          select dsp_cookie_id, adx_cookie_id, adx_type
                          from %(source_tb_cookie_rel)s
                          where day_id =%(ARG_OPTIME)s and provider = '%(provider)s' 
                                and province = '%(province)s' and dsp_type = 'ipinyou' and adx_type in ('baidu', 'ali') 
                         ) b
                     on (a.flag_id = b.adx_cookie_id and split(a.flag,'_')[1] = b.adx_type)
                     UNION ALL
                     select flag_id,type_id,tags,weight
                     from %(target_tb)s
                     where day_id =%(ARG_OPTIME)s and provider = '%(provider)s' 
                           and province = '%(province)s' and flag = 'c_ipinyou'
                   )t1
                cluster by flag_id,type_id,tags,weight
         )t2
        ''' % vars())
        HiveExe(hivesql, name, dates)




#程序结束
    End(name,dates)

#异常处理
except Exception,e:
    Except(name,dates,e)
