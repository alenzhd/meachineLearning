#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：dmp_um_user_rel_dt.py
# **  功能描述：用户关系累计表
# **  输入表： dmp_um_id_rel_bd
# **  输出表:  dmp_um_user_rel_dt
# **  创建者:  hulx
# **  创建日期: 2016/07/27
# **  修改日志:
# **  修改日期: yyyy/mm/dd，修改人 xxx  ，修改内容：
# ** ---------------------------------------------------------------------------------------
# **
# ** ---------------------------------------------------------------------------------------
# **
# **  程序调用格式：python ......py yyyymmdd(hh)
# **
#********************************************************************************************
# **  Copyright(c) 2015 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
#********************************************************************************************


import sys,os, subprocess
import datetime
import time
import xml.dom.minidom


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


#uid2id_score最小阈值
# low_threshold = 0.05


#user_name的site字典
# siteDict=dict()
# siteDict["ali"]="7"
# siteDict["qq"]="4"
# siteStr="('"+"','".join(siteDict.keys())+"')"


# def parse_xml_getrootdata(province,provider,name):
#     DOMTree = xml.dom.minidom.parse(current_path+"/../ud/ul/"+province+"_"+provider+"_config.xml")
#     collection = DOMTree.documentElement
#     dataElement = collection.getElementsByTagName(name)[0]
#     return dataElement.childNodes[0].data



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
    source_tb = "dmp_um_id_rel_dt"
    #===========================================================================================
    #自定义变量声明---目标表声明
    #===========================================================================================
    target_tb = "dmp_um_user_rel_dt"

    #===========================================================================================
    #创建创建目标表
    #===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            mix_uid string,
            flag_id string,
            id2uid_score double,
            uid2id_score double
        )
        PARTITIONED BY(provider string,province string,net_type string,day_id string,user_type string,flag string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)

#===========================================================================================
#生成表
#用户关系累计表,一个用户最对只对应一个imei等情况
#===========================================================================================    
    
    hivesql = []
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.optimize.sort.dynamic.partition=false;
        set mapreduce.map.memory.mb=2048;
        set mapreduce.reduce.memory.mb=4096;
        
        insert overwrite table %(target_tb)s
          partition(provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,user_type,flag)
        select mix_uid,flag_id,id2uid_score,uid2id_score,user_type,flag
        from (
          select mix_uid,flag_id,id2uid_score,uid2id_score,user_type,flag
                ,row_number() over (distribute by user_type,flag,flag_id sort by id2uid_score desc ) rownumber
            from %(source_tb)s
            where provider='%(provider)s' and province='%(province)s' and day_id=%(ARG_OPTIME)s 
                and net_type='%(NET_TYPE)s'
        )a where a.rownumber=1 
    ''' % vars())
    HiveExe(hivesql, name, dates)

#===============================================================================
# 移网
#===============================================================================
# user_type = mobile    flag = md532_imei
# 将std_imei进行15位md5加密，存到flag= md532_imei；
#===============================================================================
    if provider == 'dxy' and NET_TYPE == 'mobile':
        hivesql = []
        hivesql.append('''
            set hive.exec.dynamic.partition=true;
            set hive.exec.dynamic.partition.mode=nonstrict;
            set hive.optimize.sort.dynamic.partition=false;
            set mapreduce.map.memory.mb=2048;
            set mapreduce.reduce.memory.mb=4096;
            add jar %(JAR_MIX)s ;
            create temporary function Md5UDF as 'com.ai.hive.udf.util.Md5UDF';
            insert overwrite table %(target_tb)s
              partition(provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',
              day_id=%(ARG_OPTIME)s,user_type,flag)
            select mix_uid,Md5UDF(flag_id),id2uid_score,uid2id_score,user_type, 'md532_imei'
                from %(target_tb)s
                where provider='dxy' and province='%(province)s' and day_id=%(ARG_OPTIME)s
                and net_type='%(NET_TYPE)s' and flag ='std_imei'

        ''' % vars())
        HiveExe(hivesql, name, dates)




#===============================================================================
#江苏联通imei 进行md5加密  加入到  flag = stdmd5_imei分区
#===============================================================================
    elif provider == 'lt' and NET_TYPE == 'mobile':
        hivesql = []
        hivesql.append('''
            set hive.exec.dynamic.partition=true;
            set hive.exec.dynamic.partition.mode=nonstrict;
            set hive.optimize.sort.dynamic.partition=false;
            set mapreduce.map.memory.mb=2048;
            set mapreduce.reduce.memory.mb=4096;
            add jar %(JAR_MIX)s ;
            create temporary function Md5UDF as 'com.ai.hive.udf.util.Md5UDF'; 
            insert overwrite table %(target_tb)s
              partition(provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,user_type,flag)
            select mix_uid,Md5UDF(flag_id),id2uid_score,uid2id_score,'mobile','md532_imei'
            from (
              select mix_uid,flag_id,id2uid_score,uid2id_score
                from %(target_tb)s
                where provider='lt' and province='jiangsu' and day_id=%(ARG_OPTIME)s 
                    and net_type='mobile' and flag in ('imei', 's_imei') and length(flag_id) = 15
                group by mix_uid,flag_id,id2uid_score,uid2id_score
            )a
        ''' % vars())
        HiveExe(hivesql, name, dates)


#===============================================================================
#user_type=c_ipinyou增加flag=c_ipinyou 、user_type=c_ali增加flag=c_ali、user_type=c_baidu增加flag=c_baidu
#===============================================================================
    if NET_TYPE == 'adsl':
        hivesql = []
        hivesql.append('''
            set hive.exec.dynamic.partition=true;
            set hive.exec.dynamic.partition.mode=nonstrict;
            set hive.optimize.sort.dynamic.partition=false;
            set mapreduce.map.memory.mb=2048;
            set mapreduce.reduce.memory.mb=4096;
        
            insert overwrite table %(target_tb)s
            partition(provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,user_type,flag)
            select mix_uid,mix_uid,'1','1',user_type,user_type
            from (
                select mix_uid, user_type
                from dmp_um_user_dt
                where provider='%(provider)s' and province='%(province)s' and day_id=%(ARG_OPTIME)s 
                    and net_type='%(NET_TYPE)s' and user_type in ('c_ali', 'c_baidu', 'c_ipinyou')
            )a
        ''' % vars())
        HiveExe(hivesql, name, dates)

#===============================================================================
#将user_type=c_baidu/flag=cproid写入user_type=c_baidu/flag=c_baidu分区
#===============================================================================
    if NET_TYPE == 'adsl':
        hivesql = []
        hivesql.append('''
            set hive.exec.dynamic.partition=true;
            set hive.exec.dynamic.partition.mode=nonstrict;
            set hive.optimize.sort.dynamic.partition=false;
            set mapreduce.map.memory.mb=2048;
            set mapreduce.reduce.memory.mb=4096;
            insert overwrite table %(target_tb)s
            partition(provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,user_type='c_baidu',flag='c_baidu')
            select mix_uid,flag_id,id2uid_score,uid2id_score
                from %(target_tb)s
                where provider='%(provider)s' and province='%(province)s' and day_id=%(ARG_OPTIME)s
                    and net_type='%(NET_TYPE)s' and user_type='c_baidu' and (flag='c_baidu' or flag='cproid')
                    group by mix_uid,flag_id,id2uid_score,uid2id_score;
        ''' % vars())
        HiveExe(hivesql, name, dates)

#程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)

