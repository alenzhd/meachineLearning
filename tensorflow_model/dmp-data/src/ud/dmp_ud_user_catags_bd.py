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
    source_tb = 'dmp_ud_user_stdtags_bd'

    source_tb_app_rel = 'dmp_console.dim_app_category_rel'
    source_tb_app_cont_rel = 'dmp_console.dim_app_category_cont_rel'
    source_tb_site_rel = 'dmp_console.dim_site_category_rel'
    source_tb_site_cont_rel = 'dmp_console.dim_site_category_cont_rel'
    source_tb_kwtag_rel = 'dmp_console.dim_kwtag_category_rel'
    source_tb_kwtag_cont_rel = 'dmp_console.dim_kwtag_category_cont_rel'
#===========================================================================================
#自定义变量声明---目标表声明
#===========================================================================================
    target_tb = 'dmp_ud_user_catags_bd'
#===========================================================================================
#创建创建目标表
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            mix_uid   string,
            tags   string,
            weight      double,
            hour_list    string
        )
        partitioned by (provider string,province string,net_type string,day_id string,user_type string, cat_type string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)

#======================================================================
#写入原始app,site,cont,kw标签到dmp_ud_user_catags_bd
#======================================================================
    hivesql = []
    hivesql.append('''
    set hive.auto.convert.join=false;
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.optimize.sort.dynamic.partition=false;
    insert overwrite table %(target_tb)s
    partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,user_type,cat_type)
    select mix_uid, tags, weight, hour_list, user_type,if(type_id='0','cont',if(type_id='b','site', if(type_id='c','app','kw'))) as cat_type
    from(
        select mix_uid, type_id, tags, weight, hour_list, user_type
        from %(source_tb)s
        where provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s' and day_id=%(ARG_OPTIME)s
        and type_id in ('b','c','d','0')
        group by mix_uid, type_id, tags, weight, hour_list, user_type
    )t
    group by mix_uid, tags, weight, hour_list, user_type, if(type_id='0','cont',if(type_id='b','site', if(type_id='c','app','kw')))
    ''' % vars())
    HiveExe(hivesql, name, dates)

#======================================================================
#app_id,site_id,kw_id标签转成分类id到dmp_ud_user_catags_bd
#======================================================================
    hivesql = []
    hivesql.append('''
    set hive.auto.convert.join=false;
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.optimize.sort.dynamic.partition=false;
    add file %(current_path)s/merge_tags_hour_list.py;
    insert overwrite table %(target_tb)s
    partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,user_type,cat_type)
    select mix_uid, tags, weight, hour_list, user_type, cat_type
    from(
        select transform( mix_uid, cat_type, tags, hour_list, user_type)
        using 'python merge_tags_hour_list.py'
        as (mix_uid, cat_type, tags, weight, hour_list, user_type)
        from(
            select mix_uid, cat_type, tags, hour_list, user_type
            from(
                select a.mix_uid, b.category_id as tags, a.hour_list, a.user_type, a.cat_type
                from(
                    select mix_uid, tags, hour_list, user_type, cat_type
                    from %(target_tb)s
                    where cat_type = 'app' and provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s' and day_id=%(ARG_OPTIME)s
                )a
                join(
                    select app_id, category_id
                    from %(source_tb_app_rel)s
                )b
                on(a.tags = b.app_id)
                group by a.mix_uid, b.category_id, a.hour_list, a.user_type, a.cat_type

            UNION ALL

                select a.mix_uid, b.category_id as tags, a.hour_list, a.user_type, a.cat_type
                from(
                    select mix_uid, tags, hour_list, user_type, cat_type
                    from %(target_tb)s
                    where cat_type = 'site' and provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s' and day_id=%(ARG_OPTIME)s
                )a
                join(
                    select site_id, category_id
                    from %(source_tb_site_rel)s
                )b
                on(a.tags = b.site_id)
                group by a.mix_uid, b.category_id, a.hour_list, a.user_type, a.cat_type

            UNION ALL

                select a.mix_uid, b.category_id as tags, a.hour_list, a.user_type, a.cat_type
                from(
                    select mix_uid, tags, hour_list, user_type, cat_type
                    from %(target_tb)s
                    where cat_type = 'kw' and provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s' and day_id=%(ARG_OPTIME)s
                )a
                join(
                    select kwtag_id, category_id
                    from %(source_tb_kwtag_rel)s
                )b
                on(a.tags = b.kwtag_id)
                group by a.mix_uid, b.category_id, a.hour_list, a.user_type, a.cat_type

            )t1
            cluster by cat_type, user_type, mix_uid, tags
        )t2
    )t3
    ''' % vars())
    HiveExe(hivesql, name, dates)



#======================================================================
#将app,site,kw分类id转换成cont标签
#======================================================================
    hivesql = []
    hivesql.append('''
    set hive.auto.convert.join=false;
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.optimize.sort.dynamic.partition=false;
    add file %(current_path)s/merge_tags_hour_list.py;
    insert overwrite table %(target_tb)s
    partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,user_type,cat_type)
    select mix_uid, tags, weight, hour_list, user_type, cat_type
    from(
        select transform( mix_uid, cat_type, tags, hour_list, user_type)
        using 'python merge_tags_hour_list.py'
        as (mix_uid, cat_type, tags, weight, hour_list, user_type)
        from(
            select mix_uid, 'cont' as cat_type, tags, hour_list, user_type
            from(
                select a.mix_uid, b.cont_id as tags, a.hour_list, a.user_type
                from(
                    select mix_uid, tags, hour_list, user_type
                    from %(target_tb)s
                    where cat_type = 'app' and provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s' and day_id=%(ARG_OPTIME)s
                )a
                join(
                    select category_id, cont_id
                    from %(source_tb_app_cont_rel)s
                )b
                on(a.tags = b.category_id)
                group by a.mix_uid, b.cont_id, a.hour_list, a.user_type

            UNION ALL

                select a.mix_uid, b.cont_id as tags, a.hour_list, a.user_type
                from(
                    select mix_uid, tags, hour_list, user_type
                    from %(target_tb)s
                    where cat_type = 'site' and provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s' and day_id=%(ARG_OPTIME)s
                )a
                join(
                    select category_id, cont_id
                    from %(source_tb_site_cont_rel)s
                )b
                on(a.tags = b.category_id)
                group by a.mix_uid, b.cont_id, a.hour_list, a.user_type

            UNION ALL

                select a.mix_uid, b.cont_id as tags, a.hour_list, a.user_type
                from(
                    select mix_uid, tags, hour_list, user_type
                    from %(target_tb)s
                    where cat_type = 'kw' and provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s' and day_id=%(ARG_OPTIME)s
                )a
                join(
                    select category_id, cont_id
                    from %(source_tb_kwtag_cont_rel)s
                )b
                on(a.tags = b.category_id)
                group by a.mix_uid, b.cont_id, a.hour_list, a.user_type

            UNION ALL

                select mix_uid, tags, hour_list, user_type
                from %(target_tb)s
                where cat_type = 'cont' and provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s' and day_id=%(ARG_OPTIME)s

            UNION ALL

                select a.mix_uid, b.cont_id as tags, a.hour_list, a.user_type
                from(
                    select mix_uid, type_id, tags, hour_list, user_type
                    from %(source_tb)s
                    where provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s' and day_id=%(ARG_OPTIME)s
                    and type_id in ('b','c')
                )a
                join(
                    select tags, type_id, cont_id
                    from dmp_category.dim_tags_cont_rel
                    where state = '1'
                )b
                on (a.tags = b.tags and a.type_id = b.type_id)
                group by a.mix_uid, b.cont_id, a.hour_list, a.user_type
            )t1
            cluster by user_type, mix_uid, tags
        )t2
    )t3
    ''' % vars())
    HiveExe(hivesql, name, dates)

 
#程序结束
    End(name,dates)

#异常处理
except Exception,e:
    Except(name,dates,e)

