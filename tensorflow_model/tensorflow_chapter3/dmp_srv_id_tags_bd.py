#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：dmp_srv_id_tags_bd.py
# **  功能描述：dev_id/tag_name/weight  分区day_id/id_type/tag_type
# **  输入表：
# **  输出表:   dmp_srv_id_tags_bd
# **  创建者:   hulx
# **  创建日期: 2017/08/18
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
    source_tb_stdtags = "dmp_ud_user_stdtags_bd"
    source_tb_catags = "dmp_ud_user_catags_bd"
    source_tb_relobj = "dmp_um_user_relobj_bd"
    source_tb_rel = "dmp_um_user_rel_dt"

#===========================================================================================
#自定义变量声明---维表声明
#===========================================================================================
    dim_app = 'dmp_console.dim_app'
    dim_site = 'dmp_console.dim_site'
    dim_kw = 'dmp_console.dim_data_kwtag_base'
    dim_cont = 'dmp_console.dim_cont'
    dim_relobj = 'dmp_console.dim_data_pubnum_base'

#===========================================================================================
#自定义变量声明---目标表声明
#===========================================================================================
    target_tb = "dmp_srv_id_tags_bd"

#===========================================================================================
#创建创建目标表
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            dev_id   string,
            tag_id string,
            tag_name   string,
            weight    double,
            province string
        )
        partitioned by (day_id string,id_type string,tag_type string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)

#================================================================================
#
#================================================================================
    hivesql = []
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        add jar %(JAR_MIX)s;
        create temporary function Md5UDF as 'com.ai.hive.udf.util.Md5UDF';
        create temporary function SladuidEncodeUDF as 'com.ai.hive.udf.sladuid.SladuidEncodeUDF';
        create temporary function Unbases64UDF as 'com.ai.hive.udf.util.Unbases64UDF';
        insert overwrite table %(target_tb)s
        partition (day_id=%(ARG_OPTIME)s,id_type,tag_type)
        select tb2.dev_id, tb1.tags, tb1.name, max(tb1.weight),tb1.province, tb2.id_type, tb1.tag_type,
        from(
            select mix_uid, tags, name, weight, type_id as tag_type,province
            from(
                select a.mix_uid, a.tags, b.name, a.weight, if(a.type_id = 'b', 'site', if(a.type_id = 'c', 'app', 'kw')) as type_id
                from(
                    select mix_uid, tags, type_id, max(weight) as weight
                    from %(source_tb_stdtags)s
                    where day_id = %(ARG_OPTIME)s and type_id in ('b', 'c', 'd')
                    group by mix_uid, tags, type_id
                )a
                join(
                    select tags, name, type_id
                    from(
                        select app_id as tags, app_name as name, 'c' as type_id
                        from %(dim_app)s
                        where state != '0'

                        UNION ALL

                        select site_id as tags, domain as name, 'b' as type_id
                        from %(dim_site)s
                        where state != '0'

                        UNION ALL

                        select kwtag_id as tags, kw as name, 'd' as type_id
                        from %(dim_kw)s
                        where state != '0'
                    )t
                )b
                on (a.tags = b.tags and a.type_id = b.type_id)

                UNION ALL

                select a.mix_uid, a.tags, b.name, a.weight, 'cont' as type_id
                from(
                    select mix_uid, tags, weight
                    from %(source_tb_catags)s
                    where day_id = %(ARG_OPTIME)s and cat_type = 'cont' and tags like '8%%'
                    group by mix_uid, tags
                )a
                join(
                    select cont_id as tags, cont_path as name
                    from %(dim_cont)s
                    where state != '0'
                )b
                on (a.tags = b.tags)

                UNION ALL

                select a.mix_uid, a.relobj_id as tags, b.name, a.weight, 'pubnum' as type_id
                from(
                    select mix_uid, relobj_id, weight
                    from %(source_tb_relobj)s
                    where day_id = %(ARG_OPTIME)s and relobj_type = 'sladuid_pubnum'
                    group by mix_uid, relobj_id
                )a
                join(
                    select id, name
                    from(
                        select id, name
                        from %(dim_relobj)s

                        UNION ALL

                        select Unbases64UDF(id) as id, name
                        from %(dim_relobj)s
                    )relobj
                    group by id, name
                )b
                on (a.relobj_id = SladuidEncodeUDF('18',b.id))
            )t
        )tb1
        join(
            select mix_uid, lower(if(flag='idfa',Md5UDF(flag_id),flag_id)) as dev_id, if(flag='idfa','idfa','imei') as id_type
            from %(source_tb_rel)s
            where day_id = %(ARG_OPTIME)s and flag in ('md532_imei', 'idfa')
            group by mix_uid, flag_id, flag
        )tb2
        on (tb1.mix_uid = tb2.mix_uid)
        group by tb2.dev_id, tb1.tags, tb1.name, tb2.id_type, tb1.tag_type
    ''' % vars())
    HiveExe(hivesql, name, dates)


#程序结束
    End(name,dates)

#异常处理
except Exception,e:
    Except(name,dates,e)
