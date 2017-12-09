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
mix_home=current_path+'/../../../'
python_path = []
python_path.append(mix_home+'/conf')
sys.path[:0]=python_path

#引入自定义包
from settings import *

#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
hourid = sys.argv[3]
dates = hourid[0:8]
provider = PROVIDER
province = PROVINCE

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
    #===========================================================================================
    #自定义变量声明---目标表声明
    target_tb = "dmp_uc_tags_kw_m_bh"
    target_tmp = "dmp_uc_tags_kw_m_bh_tmp"
    #===========================================================================================
    #创建创建目标表
    #===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            user_id string,
            value string,
            segment string,
            count string
        )
        partitioned by (provider string,province string,day_id string,hour_id string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    hivesql.append('''
        create table if not exists %(target_tmp)s
        (
            mix_m_uid string,
            value string,
            count string
        )
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tmp)s';
    ''' % vars())
    #执行hivesql语句
    HiveExe(hivesql,name,dates)
    #===========================================================================================
    #插入目标表
    #===========================================================================================
    #过滤锯齿形数据
    hivesql = []
    hivesql.append('''
        add file %(current_path)s/filter_sawtooth.py;
        insert overwrite table %(target_tmp)s
        select mix_m_uid,concat_ws(':',value,cast (sum(count) as string))
        from
        (
            select transform (mix_m_uid,value,count,last_timestamp,day_id)
            using 'python filter_sawtooth.py'
            as(mix_m_uid,value,count) from
            (
                select * from %(source_tb)s
                where hour_id=%(hourid)s and value_type_id =1 sort by mix_m_uid,last_timestamp
            ) a
        ) b group by mix_m_uid,value,day_id
    ''' % vars())
    HiveExe(hivesql, name, dates)
    #分词
    hivesql = []
    hivesql.append('''
        add file %(current_path)s/wordSegment.py;
        add file %(current_path)s/stopword.data;
        add file %(current_path)s/userdict.txt;
        insert overwrite table %(target_tb)s partition(provider='%(provider)s',province='%(province)s',day_id=%(ARG_OPTIME)s,hour_id=%(hourid)s)
        select transform (mix_m_uid,value,count)
        using 'python wordSegment.py'
        as(user_id,value,segment,count)
        from %(target_tmp)s
    ''' % vars())
    HiveExe(hivesql,name,dates)
    #===========================================================================================
    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)

