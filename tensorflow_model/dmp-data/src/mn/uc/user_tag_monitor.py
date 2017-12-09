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
# ** ！！！！！提高效率修改方案：减少数据扫描次数，用hive streaming来做。
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
mix_home=current_path+'/../../../'
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
    source_tb = "dmp_uc_tags_m_bh"
    source_flow_loc = "dmp_uc_otags_m_bh"
    #===========================================================================================
    #自定义变量声明---目标表声明
    target_tb = "dmp_mn_kpi_bd"
    #===========================================================================================
    #创建创建目标表
    #===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            kpi string,
            value double
        )
        partitioned by (provider string,province string,net_type string,day_id string,module string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s'
    ''' % vars())
    #执行hivesql语句
    HiveExe(hivesql,name,dates)
    #===========================================================================================
    #插入目标表
    #===========================================================================================
    #有标签用户
    hivesql = []

    flag_arr={'search':'uc_2','goods_id':'uc_3','app_id':'uc_4','web':'uc_5','flow':'uc_6'}

    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert overwrite table %(target_tb)s partition(provider,province,net_type,day_id,module)
        select kpi,value,provider,province,net_type,day_id,module
        from(
            select  concat_ws('|','uc',name,'','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                    count(distinct mix_m_uid) as value,
                    provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc' as module
            from (
                select mix_m_uid,province,provider,
                        case when value_type_id= 1 then 'uc_2' when value_type_id= 2 then 'uc_3' else '' end as name
                from %(source_tb)s where day_id=%(ARG_OPTIME)s and value != ''
                and province = '%(province)s' and provider = '%(provider)s'
            ) aa where name != '' group by province,provider,name

            union all

            select  concat_ws('|','uc','uc_1','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                count(distinct mix_m_uid) as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc' as module
            from %(source_tb)s where day_id=%(ARG_OPTIME)s and province = '%(province)s' and provider = '%(provider)s'
            group by provider,province

            union all

            select  concat_ws('|','uc','uc_4','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                count(distinct mix_m_uid) as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc' as module
            from(
               select mix_m_uid,province,provider from %(source_tb)s where app_id != '' and day_id=%(ARG_OPTIME)s
               and province = '%(province)s' and provider = '%(provider)s'
            ) aa group by provider,province

            union all

            select  concat_ws('|','uc','uc_5','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                count(distinct mix_m_uid) as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc' as module
            from(
               select mix_m_uid,province,provider from %(source_tb)s where cont_id != '' and day_id=%(ARG_OPTIME)s
               and province = '%(province)s' and provider = '%(provider)s'
            ) aa group by provider,province

            union all

            select  concat_ws('|','uc','uc_7','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                count(distinct mix_m_uid) as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc' as module
            from(
               select mix_m_uid,province,provider from %(source_flow_loc)s where sa_id ='loc_id' and tag_index != ''
               and day_id=%(ARG_OPTIME)s and province = '%(province)s' and provider = '%(provider)s'
            ) aa group by provider,province

            union all

            select  concat_ws('|','uc','uc_8','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                count(distinct mix_m_uid) as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc' as module
            from(
               select mix_m_uid,province,provider from %(source_tb)s where site_id != '' and day_id=%(ARG_OPTIME)s
               and province = '%(province)s' and provider = '%(provider)s'
            ) aa group by provider,province

            union all

            select  concat_ws('|','uc','uc_9','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                count(distinct mix_m_uid) as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc' as module
            from(
               select mix_m_uid,province,provider from %(source_tb)s where action_id != ''
               and day_id=%(ARG_OPTIME)s and province = '%(province)s' and provider = '%(provider)s'
            ) aa group by provider,province

            union all

            select  concat_ws('|','uc','uc_10','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                count(distinct mix_m_uid) as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc' as module
            from(
               select mix_m_uid,province,provider from %(source_flow_loc)s where sa_id='device_model' and tag_index != ''
               and day_id=%(ARG_OPTIME)s and province = '%(province)s' and provider = '%(provider)s'
            ) aa group by provider,province

            union all

            select  concat_ws('|','uc','uc_11','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                count(distinct mix_m_uid) as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc' as module
            from(
               select mix_m_uid,province,provider from %(source_flow_loc)s where sa_id='device_type' and tag_index != ''
               and day_id=%(ARG_OPTIME)s and province = '%(province)s' and provider = '%(provider)s'
            ) aa group by provider,province

            union all

            select  concat_ws('|','uc','uc_12','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                count(distinct mix_m_uid) as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc' as module
            from(
               select mix_m_uid,province,provider from %(source_flow_loc)s where sa_id='device_os' and tag_index != ''
               and day_id=%(ARG_OPTIME)s and province = '%(province)s' and provider = '%(provider)s'
            ) aa group by provider,province

            union all

            select  concat_ws('|','uc','uc_13','','','%(ARG_OPTIME)s',province,provider,'mobile','click','mobile','day') as kpi,
                count(distinct mix_m_uid) as value,
                provider,province,'mobile' as net_type,%(ARG_OPTIME)s as day_id,'uc' as module
            from(
               select mix_m_uid,province,provider from %(source_flow_loc)s where sa_id='device_browser' and tag_index != ''
               and day_id=%(ARG_OPTIME)s and province = '%(province)s' and provider = '%(provider)s'
            ) aa group by provider,province
        ) kk
    ''' % vars())

    HiveExe(hivesql,name,dates)
    #===========================================================================================
    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
