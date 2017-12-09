#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：ad属性计算（家庭级公司级）
# **  输入表：  dmp_um_user_bd
# **  输出表:   dmp_um_user_dt
# **  创建者:   zhangqn
# **  创建日期: 2016/03/20
# **  修改日志:
# **  修改日期: yyyy/mm/dd，修改人 xxx  ，修改内容：
# ** ---------------------------------------------------------------------------------------
# **
# ** ---------------------------------------------------------------------------------------
# **
# **  程序调用格式：python ......py yyyymmdd(hh)
# **
#********************************************************************************************
# **  Copyright(c) 2016 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
#********************************************************************************************

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
    source_tb = "dmp_um_id_rel_bd"
#===========================================================================================
#自定义变量声明---目标表声明
#===========================================================================================
    target_ad_tb = "dmp_um_ad_dt"
    target_tb = "dmp_um_user_dt"
    
#===========================================================================================
#创建创建目标表
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            mix_uid   string,
            weight    double,
            create_date    string,
            last_date    string,
            property1 string,
            property2 string
        )
        partitioned by (provider string,province string,net_type string,day_id string,user_type string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)

    hivesql = []
    hivesql.append('''
        create table if not exists %(target_ad_tb)s
        (
            mix_uid string,
            uv int,
            rest_uv int,
            rest_ratio double,
            ip_count int,
            ip_maxduration int,
            ip_medianduration int,
            ip_daycount int
        )
        partitioned by (provider string,province string,day_id string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_ad_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)

    back_day = get_date_of_back_someday(ARG_OPTIME, 29)
    back_fourteenday = get_date_of_back_someday(ARG_OPTIME, 13)
    RATIO_LE = 0.2
    UV_GT = 5
    delta = 4

    hivesql = []
    hivesql.append('''
        add file %(current_path)s/prepare_ad_property.py;
        insert overwrite table %(target_ad_tb)s
        partition (provider = '%(provider)s',province = '%(province)s', day_id=%(ARG_OPTIME)s)
        select mix_uid,max(uv),max(rest_uv),max(rest_ratio),max(ip_count),max(ip_maxduration),max(ip_medianduration),max(ip_daycount)
        from (select transform (mix_uid,concat_ws('|',collect_set(concat(flag_id,',',weight,',',day_id))))
            using 'python prepare_ad_property.py'
            as (mix_uid,uv,rest_uv,rest_ratio,ip_count,ip_maxduration,ip_medianduration,ip_daycount)
            from %(source_tb)s
            where provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s'
                and day_id<=%(ARG_OPTIME)s and day_id>=%(back_fourteenday)s
                and user_type = 'ad' and flag = 'ip'
            group by mix_uid
            union all
            select mix_uid,max(uv) as uv,max(rest_uv) as rest_uv,max(rest_uv)/max(uv) as rest_ratio,
                0 as ip_count,0 as ip_maxduration,0 as ip_medianduration,0 as ip_daycount
            from (select mix_uid,flag,count(flag_id) as uv,sum(rest) as rest_uv
                from (select mix_uid,flag_id,rest,flag
                    from (select mix_uid,flag_id,if(substring(hour_list,1,7)='0000000' and substring(hour_list,22,3)='000',0,1) as rest,flag
                        from %(source_tb)s
                        where provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s'
                            and day_id<=%(ARG_OPTIME)s and day_id>=%(back_fourteenday)s
                            and user_type = 'ad' and flag != 'sladuid' and flag != 'ip') a
                    group by mix_uid,flag_id,rest,flag) b
                group by mix_uid,flag) c
            group by mix_uid) d
        group by mix_uid
    ''' % vars())
    HiveExe(hivesql, name, dates)

    hivesql = []
    hivesql.append('''
        insert overwrite table %(target_tb)s
        partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,user_type='ad')
        select a.mix_uid,a.weight,a.create_date,a.last_date,if(b.property1 is null,'',b.property1),if(b.property2 is null,'',b.property2)
        from (select mix_uid,weight,create_date,last_date
            from %(target_tb)s
            where provider = '%(provider)s' and province = '%(province)s' and net_type='%(NET_TYPE)s'
                and day_id=%(ARG_OPTIME)s and user_type = 'ad') a
        left outer join (select mix_uid,
                CASE
                    WHEN rest_ratio<=%(RATIO_LE)s and uv>=%(UV_GT)s and ip_count=1 THEN 'company'
                    WHEN uv=0 or ip_count=0 THEN ''
                    ELSE 'home'
                END as property1,
                if (ip_count=0,'',cast(1-pow(ip_medianduration*ip_maxduration/ip_daycount/ip_daycount*(1+%(delta)s)/(ip_count+%(delta)s),1.0/3) as string)) as property2
            from %(target_ad_tb)s
            where provider = '%(provider)s' and province = '%(province)s' and day_id=%(ARG_OPTIME)s) b
        on a.mix_uid=b.mix_uid
    ''' % vars())
    HiveExe(hivesql, name, dates)


    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
