#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：dmp_srv_idmapping_bd.py
# **  功能描述：idmapping接口
# **  输入表：  dmp_um_user_rel_dt
# **  输出表:   dmp_srv_idmapping_bd, dmp_srv_idmapping_dt
# **  创建者:   hulx
# **  创建日期: 2017/03/10
# **  修改日志:
# **  修改日期: yyyy/mm/dd，修改人 xxx  ，修改内容： 
# ** ---------------------------------------------------------------------------------------
# **
# ** ---------------------------------------------------------------------------------------
# **
# **  程序调用格式：python ......py yyyymmdd
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
mix_home=current_path+'/../../../../'
python_path = []
python_path.append(mix_home+'conf')
sys.path[:0]=python_path
print sys.path

#引入自定义包
from settings import *


#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
dates = sys.argv[4]

try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']
    ARG_OPTIME_LASTDAY = dicts['ARG_OPTIME_LASTDAY']
    #获取前天日期
    dictss={}
    Pama(dictss,ARG_OPTIME_LASTDAY)
    ARG_OPTIME_LAST2DAY = dictss['ARG_OPTIME_LASTDAY']
#===========================================================================================
#自定义变量声明---源表声明
#===========================================================================================
    source_tb = 'dmp_um_user_rel_dt'
#===========================================================================================
#自定义变量声明---目标表声明
#===========================================================================================
    target_bd_tb = 'dmp_srv_idmapping_bd'
    target_dt_tb = 'dmp_srv_idmapping_dt'
#===========================================================================================
#添加streaming文件和相应的模型文件
#===========================================================================================
    add_idmapping_file = 'add file ' + current_path + '/deal_idmapping.py;'
    add_udf_jar = "add jar " + JAR_MIX + ";create temporary function row_number as 'com.ai.hive.udf.util.RowNumberUDF';"
#===========================================================================================
#创建目标表
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_bd_tb)s
        (
            key string,
            value string
        )
        partitioned by (day_id string,type string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_bd_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)

    hivesql = []
    hivesql.append('''
        create table if not exists %(target_dt_tb)s
        (
            key string,
            value string
        )
        partitioned by (day_id string,type string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_dt_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)

#======================================================================
#生成dmp_srv_idmapping_bd表
#======================================================================
    uid2id_con = "'std_imei','idfa','mac','cuid'"
    id2uid_con = "'std_imei','idfa','mac','cuid'"

    hivesql = []
    hivesql.append('''
    %(add_idmapping_file)s;
    %(add_udf_jar)s;
    SET hive.exec.compress.output=false;
    insert overwrite table %(target_bd_tb)s
    partition (day_id=%(ARG_OPTIME)s, type)
    select a.key, a.value, a.type
    from(
        select transform(mix_uid, flag_id, flag, 'uid2id')
        using 'python deal_idmapping.py'
        as (key, value, type)
        from(
            select mix_uid, flag_id, flag
            from(
                select mix_uid, flag_id, uid2id_score, if(flag='std_imei','imei',flag) as flag
                from %(source_tb)s
                where day_id >= %(ARG_OPTIME_LAST2DAY)s and day_id <= %(ARG_OPTIME_LASTDAY)s
                    and user_type = 'mobile' and flag in (%(uid2id_con)s)
                distribute by mix_uid, flag
                sort by mix_uid, flag, uid2id_score desc
            )t1
            where row_number(mix_uid, flag) = 1
            order by mix_uid, flag
        )t11

        UNION ALL

        select transform(mix_uid, flag_id, flag, 'id2uid')
        using 'python deal_idmapping.py'
        as (key, value, type)
        from(
            select mix_uid, flag_id, flag
            from(
                select mix_uid, flag_id, id2uid_score, if(flag='std_imei','imei',flag) as flag
                from %(source_tb)s
                where day_id >= %(ARG_OPTIME_LAST2DAY)s and day_id <= %(ARG_OPTIME_LASTDAY)s
                    and user_type = 'mobile' and flag in (%(id2uid_con)s)
                distribute by flag, flag_id
                sort by flag, flag_id, id2uid_score desc
            )t2
            where row_number(flag, flag_id) = 1
        )t22
    )a
    left join(
        select key, value, type
        from %(target_dt_tb)s
        where day_id = %(ARG_OPTIME_LASTDAY)s
    )b
    on (a.key = b.key and a.value = b.value and a.type = b.type)
    where b.key is null or b.value is null or b.type is null
    group by a.key, a.value, a.type
    ''' % vars())
    HiveExe(hivesql, name, dates)


#======================================================================
#生成dmp_srv_idmapping_dt表
#======================================================================
    hivesql = []
    hivesql.append('''
    insert overwrite table %(target_dt_tb)s
    partition (day_id = %(ARG_OPTIME)s,type)
    select key, value, type
    from(
        select b.key as key, b.value as value, b.type as type
        from(
            select key, type
                from %(target_bd_tb)s
                where day_id = %(ARG_OPTIME)s
                group by key, type
        )a
        right join(
            select key, value, type
                from %(target_dt_tb)s
                where day_id = %(ARG_OPTIME_LASTDAY)s
        )b
        on (a.key = b.key and a.type = b.type)
        where a.key is null and a.type is null

        UNION ALL

        select key, value, type
            from %(target_bd_tb)s
            where day_id = %(ARG_OPTIME)s
    )t2
    group by key, value, type
    ''' % vars())
    HiveExe(hivesql, name, dates)

#程序结束
    End(name,dates)

#异常处理
except Exception,e:
    Except(name,dates,e)


