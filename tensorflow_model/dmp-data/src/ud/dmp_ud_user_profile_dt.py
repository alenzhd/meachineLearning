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

import sys,os, subprocess
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
    source_tb_cat_bd = 'dmp_ud_user_catags_bd'
    source_tb_std_dt = 'dmp_ud_user_stdtags_dt'
#===========================================================================================
#自定义变量声明---目标表声明
#===========================================================================================
    target_tb = 'dmp_ud_user_profile_dt'
#===========================================================================================
#添加streaming文件和相应的模型文件
#===========================================================================================
    add_gender_model = ' add file ' + current_path + '/deal_gender.py ; add file ' + current_path + '/gender_feature_model.txt ;'
    add_age_model = ' add file ' + current_path + '/deal_age.py ; add file ' + current_path + '/age_feature_model.txt ;'
    add_dev_file = 'add file ' + current_path + '/deal_dev.py'
#===========================================================================================
#因采取省份轮休，获取最近一天的日期
#===========================================================================================
    filepath = HIVE_TB_HOME + '/' + source_tb_std_dt + '/provider='+provider+'/province='+province+'/net_type='+NET_TYPE
    cat = subprocess.Popen(['hadoop', 'fs', '-du',filepath],stdout=subprocess.PIPE)
    res = ARG_OPTIME_LASTDAY
    for line in cat.stdout:
        if 'day_id=' in line and int(line.split(' ')[0].strip()) >= 1048576:
            tmp = line.split('=')[-1].strip()
            if int(tmp) <= int(ARG_OPTIME):
                res = tmp
    ARG_OPTIME_DATA_LASTDAY = res.strip()
#===========
    if NET_TYPE == 'adsl':
        user_type = 'ad'
    else:
        user_type = 'mobile'
#===========
#===========================================================================================
#创建创建目标表
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            mix_uid string,
            tags    string,
            score   double
        )
        partitioned by (provider string,province string,net_type string,day_id string,user_type string,profile_type string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)

#======================================================================
#device
#======================================================================
    hivesql = []
    hivesql.append('''
    set hive.auto.convert.join=false;
    set hive.groupby.skewindata=true;
    %(add_dev_file)s;
    insert overwrite table %(target_tb)s
    partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,user_type='%(user_type)s', profile_type='dev')
    select transform(mix_uid, tags, score)
    using 'python deal_dev.py'
    as (mix_uid, tags, score)
    from(
        select mix_uid, substr(tags,0,3), tags, score
        from %(source_tb_std_dt)s
        where provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s'
            and day_id=%(ARG_OPTIME_DATA_LASTDAY)s and type_id = '4' and user_type in ('ad', 'mobile')
        distribute by mix_uid, substr(tags,0,3)
        sort by mix_uid, substr(tags,0,3), score desc
    )t
    ''' % vars())
    HiveExe(hivesql, name, dates)

#======================================================================
#cont
#======================================================================
    hivesql = []
    hivesql.append('''
    set hive.auto.convert.join=false;
    set hive.groupby.skewindata=true;
    insert overwrite table %(target_tb)s
    partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,user_type='%(user_type)s', profile_type='cont')
    select mix_uid, tags, sum(weight)
    from(
        select mix_uid, tags, score * exp(-0.1) as weight, user_type
        from %(target_tb)s
            where provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s'
            and day_id=%(ARG_OPTIME_DATA_LASTDAY)s and profile_type = 'cont' and user_type in ('ad','mobile')
            and score * exp(-0.1) > exp(-18)
        UNION ALL
        select mix_uid, tags, weight, user_type
        from %(source_tb_cat_bd)s
            where provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s'
            and day_id=%(ARG_OPTIME)s and cat_type = 'cont' and user_type in ('ad','mobile')
    )t
    group by mix_uid, tags
    ''' % vars())
    HiveExe(hivesql, name, dates)

#======================================================================
#gender
#======================================================================
    hivesql = []
    hivesql.append('''
    set hive.auto.convert.join=false;
    set hive.groupby.skewindata=true;
    %(add_gender_model)s;
    insert overwrite table %(target_tb)s
    partition (provider = '%(provider)s',province = '%(province)s',net_type = '%(NET_TYPE)s',day_id = %(ARG_OPTIME)s, user_type='%(user_type)s', profile_type='dg_gender')
    select mix_uid, tags, score
    from(
        select transform(mix_uid, type_id, tags, user_type) using 'python deal_gender.py' as (mix_uid, tags, score, user_type)
        from(
            select mix_uid, type_id, tags, user_type
            from(
                select mix_uid, type_id, tags, user_type
                from %(source_tb_std_dt)s
                    where day_id = %(ARG_OPTIME_DATA_LASTDAY)s and provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s' and user_type in ('ad','mobile')
                    group by mix_uid, type_id, tags, user_type
            )t1
            cluster by user_type, mix_uid
        )t2
    )t3
    group by mix_uid, tags, score
    ''' % vars())
    HiveExe(hivesql, name, dates)

#======================================================================
#age
#======================================================================
    hivesql = []
    hivesql.append('''
    set hive.auto.convert.join=false;
    set hive.groupby.skewindata=true;
    %(add_age_model)s;
    insert overwrite table %(target_tb)s
    partition (provider = '%(provider)s',province = '%(province)s',net_type = '%(NET_TYPE)s',day_id = %(ARG_OPTIME)s, user_type='%(user_type)s', profile_type='dg_age')
    select  mix_uid, tags, score
    from(
        select transform(mix_uid, type_id, tags, score, user_type) using 'python deal_age.py' as (mix_uid, tags, score, user_type)
        from(
            select mix_uid, type_id, tags, score, user_type
            from(
                select mix_uid, type_id, tags,score, user_type
                from %(source_tb_std_dt)s
                    where day_id = %(ARG_OPTIME_DATA_LASTDAY)s and provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s' and user_type in ('ad','mobile')
                    group by mix_uid, type_id, tags,score, user_type
            )t1
            cluster by user_type, mix_uid
        )t2
    )t3
    group by mix_uid, tags, score
    ''' % vars())
    HiveExe(hivesql, name, dates)


#程序结束
    End(name,dates)

#异常处理
except Exception,e:
    Except(name,dates,e)

