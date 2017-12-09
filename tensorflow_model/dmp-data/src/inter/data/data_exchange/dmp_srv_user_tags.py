#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：dmp_srv_user_tags.py
# **  功能描述：获取个人关注标签数据，插入到个人关注表。
# **            商品信息入库。整理为key-value格式上传到待传redis表
# **  输入表：  
# **          
# **  调用格式：   python dmp_srv_user_tags.py $yyyyMMdd
# **                     
# **  输出表： 
# **           
# **  创建者:   ymh
# **  创建日期: 2015/08/26
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
python_path = []
python_path.append(mix_home+'/conf')
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
dates = sys.argv[3][0:8]
#hourid=sys.argv[1][8:10]
before3days=get_date_of_back_someday(dates,3)
try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']
    
#===========================================================================================
#自定义变量声明---输入结果表声明
#===========================================================================================
    source_tb_dsp_log = "dsp.dmp_api_query_log"
    source_tb_jiangsu_lt= "dmp_ud_ul_user_portrait"
    dmp_um_m_ht ="dmp_um_m_ht"
#===========================================================================================
#自定义变量声明---输出结果表声明
#===========================================================================================
    dmp_log_tags = "dmp_srv_user_tags"
#创建目标表dmp_srv_user_tags
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(dmp_log_tags)s (
            key        string,
            name	string,
            value	string,
            createtime string
        )partitioned by (provider string,province string,type_id string,keytype string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(dmp_log_tags)s'
    '''% vars())
    HiveExe(hivesql,name,dates)
    
#===========================================================================================
#将log内涉及到的个人关注标签提取
#1、提取log内非江苏联通信息，覆盖到相应分区
#2、将江苏联通个人关注信息直接转换导入
#3、将所有分区内数据去重，涉及到数据最新问题，以createtime为准。
#===========================================================================================

    
    #===========================================================================================
    #1、插入除-江苏联通-新数据
    #===========================================================================================
 
    hivesql = []
    hivesql.append('''
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    add file %(current_path)s/split_json_personal_map.py ;
    insert into table %(dmp_log_tags)s partition(provider,province,type_id,keytype)
    select t.key,t.name,t.value,t.createtime,t.provider,t.province,t.type_id,t.keytype from
    (select transform(key,keytype,keyvalue,cretetime,ctype,province) using 'python split_json_personal_map.py' 
    as (key,name,value,createtime,provider,province,type_id,keytype)
    from %(source_tb_dsp_log)s where day_id<=%(dates)s and day_id>=%(before3days)s
    )t 
    where not(t.provider='lt' and t.province='jiangsu')
    group by t.key,t.name,t.value,t.createtime,t.provider,t.province,t.type_id,t.keytype          
    '''% vars())
    HiveExe(hivesql,name,dates)
    
    
    #===========================================================================================
    #2、插入江苏联通数据
    #===========================================================================================
 
    hivesql = []
    hivesql.append('''
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table %(dmp_log_tags)s partition(provider,province,type_id,keytype)
    select t2.m_id,t1.label,t1.weight,t1.day_id,t2.provider,t2.province,'0' as type_id,'imei' as keytype
     from %(source_tb_jiangsu_lt)s t1 join %(dmp_um_m_ht)s t2 on(t1.userid=t2.mix_m_uid and t1.day_id=t2.day_id )
      where t2.provider='lt' and t2.province='jiangsu' and t2.mflag='imei' and t1.day_id<=%(dates)s and t1.day_id>=%(before3days)s and t1.type='personal' 
     group by t2.m_id,t1.label,t1.weight,t1.day_id,t2.provider,t2.province               
    '''% vars())
    HiveExe(hivesql,name,dates)
    
    hivesql = []
    hivesql.append('''
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table %(dmp_log_tags)s partition(provider,province,type_id,keytype)
    select t2.m_id,t1.label,t1.weight,t1.day_id,t2.provider,t2.province,'0' as type_id,'aid' as keytype
     from %(source_tb_jiangsu_lt)s t1 join %(dmp_um_m_ht)s t2 on(t1.userid=t2.mix_m_uid and t1.day_id=t2.day_id )
      where t2.provider='lt' and t2.province='jiangsu' and t2.mflag='android_id' and t1.day_id<=%(dates)s and t1.day_id>=%(before3days)s and t1.type='personal' 
     group by t2.m_id,t1.label,t1.weight,t1.day_id,t2.provider,t2.province               
    '''% vars())
    HiveExe(hivesql,name,dates)
    
    hivesql = []
    hivesql.append('''
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table %(dmp_log_tags)s partition(provider,province,type_id,keytype)
    select t2.m_id,t1.label,t1.weight,t1.day_id,t2.provider,t2.province,'0' as type_id,'idfa' as keytype
     from %(source_tb_jiangsu_lt)s t1 join %(dmp_um_m_ht)s t2 on(t1.userid=t2.mix_m_uid and t1.day_id=t2.day_id )
      where t2.provider='lt' and t2.province='jiangsu' and t2.mflag='idfa' and t1.day_id<=%(dates)s and t1.day_id>=%(before3days)s and t1.type='personal' 
     group by t2.m_id,t1.label,t1.weight,t1.day_id,t2.provider,t2.province               
    '''% vars())
    HiveExe(hivesql,name,dates)
    #===========================================================================================
    #3、清理原有数据-剔除过期数据（新记录中有createtime更新）
    #===========================================================================================
    hivesql = []
    hivesql.append('''
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    add file  %(current_path)s/split_json_personal_map.py ;
    add jar %(mix_home)s/lib/dmp-data-udf-0.0.1-SNAPSHOT.jar;
    create temporary function RowNumberUDF as 'com.ai.hive.udf.util.RowNumberUDF';
    insert overwrite table %(dmp_log_tags)s partition(provider,province,type_id,keytype)
    select t4.key,t4.name,t4.value,t4.createtime,t4.provider,t4.province,t4.type_id,t4.keytype from
    (select key,name,value,createtime,provider,province,type_id,keytype from %(dmp_log_tags)s 
    distribute by key,provider,province,type_id,keytype sort by key,createtime desc) t4 where RowNumberUDF(key)<=10       
    '''% vars())
    HiveExe(hivesql,name,dates)
   #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
