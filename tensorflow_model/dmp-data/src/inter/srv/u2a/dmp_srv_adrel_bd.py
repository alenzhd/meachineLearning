#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：dmp_um_id_rel_bd.py
# **  功能描述：
# **  特殊说明：
# **  输入表：  
# **          
# **  调用格式：   dmp_um_id_rel_bd.py provider province net_type date
# **                     
# **  输出表： 
# **           
# **  创建者:   hlx
# **  创建日期: 2016/03/30
# **  修改日志:
# **  修改日期: 修改人: 修改内容:
# ** ---------------------------------------------------------------------------------------
# **  
# ** ---------------------------------------------------------------------------------------
# **  
# **    
#********************************************************************************************
# **  Copyright(c) 2013 AsiaInfo Technologies (China), Inc. 
# **  All Rights Reserved.
#********************************************************************************************
import os,sys
import datetime
import time
#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../../../'
tmpdata_path=mix_home+'/tmp/'
python_path = []
python_path.append(mix_home+'/conf')
sys.path[:0]=python_path
print sys.path
#引入自定义包
from settings import *
  
#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
provider = sys.argv[1]
province = sys.argv[2]
net_type = sys.argv[3]
dates = sys.argv[4]

try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME'] 
    ARG_OPTIME_LASTDAY = dicts['ARG_OPTIME_LASTDAY']
    source_table = DXY_DATASOURCE+".dmp_um_id_rel_bd"
    target_table = "dmp_srv_adrel_bd"

#==========================================================================================
#建表
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_table)s (key_id string,  value string) 
        partitioned by (provider string, province string, net_type string, day_id string, flag string) 
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
        location '%(HIVE_TB_HOME)s/%(target_table)s';
    '''% vars())
    HiveExe(hivesql,name,dates)

#==========================================================================================
#UTDF
#===========================================================================================
    if net_type == 'adsl':
        hivesql = []
        hivesql.append('''
            set hive.exec.compress.output=false;
            set mapreduce.map.output.compress=false;
            set mapreduce.output.fileoutputformat.compress=false;
            set hive.exec.parallel=true;
            set hive.exec.parallel.thread.number=16;
            set hive.exec.dynamic.partition=true;
            set hive.exec.dynamic.partition.mode=nonstrict;
            add jar %(JAR_MIX)s;
            set mapreduce.map.memory.mb=2048;
            set mapreduce.reduce.memory.mb=3048;
            set mapreduce.map.java.opts=-Xmx2000m;
            set mapreduce.reduce.java.opts=-Xmx3048m;
            create temporary function AdrelUDTF as 'com.ai.hive.udf.inter.AdrelUDTF';
            create temporary function Md5UDF as 'com.ai.hive.udf.util.Md5UDF';
            insert overwrite table %(target_table)s
            partition(provider,province,net_type, day_id, flag)
            select key_id, value, '%(provider)s' as provider, '%(province)s' as province, '%(net_type)s' as net_type, %(dates)s as day_id, flag
            from(
                select AdrelUDTF(t1.mix_uid,t1.flag_id,t1.hour_list,t1.day_id,t1.flag) 
                   as (key_id,value,flag)
                from(
                    select t.mix_uid,t.flag_id,t.hour_list,t.day_id,t.flag
                    from (
                      select a.mix_uid as mix_uid,b.dsp_cookie_id as flag_id,a.hour_list as hour_list,a.day_id as day_id, 'c_ipinyou' as flag
                      from (
                          select mix_uid,flag_id,hour_list,day_id,flag
                          from %(source_table)s
                              where day_id = %(ARG_OPTIME)s and provider = '%(provider)s'
                                 and province = '%(province)s' and net_type = '%(net_type)s'
                                 and user_type='ad' and flag in ('c_ali', 'c_baidu')
                           )a
                           join (
                           select dsp_cookie_id, adx_cookie_id, adx_type
                           from dmp_um_cookie_rel_dt
                           where day_id =%(ARG_OPTIME)s and provider = '%(provider)s'
                                 and province = '%(province)s'
                                 and dsp_type = 'ipinyou' and adx_type in ('baidu', 'ali')
                           )b
                       on(a.flag_id = b.adx_cookie_id and split(a.flag, '_')[1] = b.adx_type)
                       UNION ALL 
                       select mix_uid,
                              if(flag = 'imei', Md5UDF(flag_id), flag_id) as flag_id,
                              hour_list,day_id,flag 
                       from %(source_table)s 
                       where day_id = %(ARG_OPTIME)s and user_type='ad' 
                             and provider = '%(provider)s' and province = '%(province)s' and net_type = '%(net_type)s'
                             and (flag in ('idfa','c_ipinyou') or (flag = 'imei' and length(flag_id)=15))
                    ) t  
                   cluster by t.flag,t.flag_id
                )t1
            )t2
        ''' % vars())
        HiveExe(hivesql,name,dates)
   
    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
