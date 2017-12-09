#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：dmp_um_id_rel_dt.py
# **  功能描述：用户关系累计表
# **  输入表： dmp_um_id_rel_bd
# **  输出表:  dmp_um_id_rel_dt
# **  创建者:  guojy
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
    source_tb = "dmp_um_user_relobj_bd"
    #===========================================================================================
    #自定义变量声明---目标表声明
    #===========================================================================================
    target_tb = "dmp_um_user_relobj_dt"
    #===========================================================================================
    #因采取省份轮休，获取最近一天的日期
    #===========================================================================================
    filepath = HIVE_TB_HOME + '/' + target_tb + '/provider='+provider+'/province='+province+'/net_type='+NET_TYPE
    cat = subprocess.Popen(['hadoop', 'fs', '-du',filepath],stdout=subprocess.PIPE)
    res = ARG_OPTIME_LASTDAY
    for line in cat.stdout:
        if 'day_id=' in line and int(line.split(' ')[0].strip()) >= 1048576:
            tmp = line.split('=')[-1].strip()
            if int(tmp) < int(ARG_OPTIME):
                res = tmp
    ARG_OPTIME_DATA_LASTDAY = res.strip()

    #===========================================================================================
    #创建创建目标表
    #===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            mix_uid string,
            relobj_id string,
            id2uid_score double,
            uid2id_score double
        )
        PARTITIONED BY(provider string,province string,net_type string,day_id string,user_type string,relobj_type string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)

#===========================================================================================
#所有用户关系累计表,存储所有用户关系，可能存在一个用户多个imei等情况
#===========================================================================================
    # back_day = get_date_of_back_someday(ARG_OPTIME, 29)
    # back_sevenday = get_date_of_back_someday(ARG_OPTIME, 6)

    hivesql = []
    hivesql.append('''
        add file %(current_path)s/cal_uidid_score.py;
        add file %(current_path)s/cal_iduid_score.py;
        set hive.exec.parallel=true;
        set hive.exec.parallel.thread.number=16;
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert overwrite table %(target_tb)s
        partition (provider = '%(provider)s',province = '%(province)s',net_type='%(NET_TYPE)s',day_id=%(ARG_OPTIME)s,user_type,relobj_type)
        select mix_uid, relobj_id, sum(id2uid_score), sum(uid2id_score), user_type, relobj_type
        from(
            select mix_uid, relobj_id, id2uid_score * exp(-0.1) as id2uid_score, uid2id_score * exp(-0.1) as uid2id_score, user_type, relobj_type
            from %(target_tb)s
            where day_id = %(ARG_OPTIME_DATA_LASTDAY)s
                and provider = '%(provider)s' and province = '%(province)s'
                and net_type='%(NET_TYPE)s' and mix_uid!='' and relobj_id != ''
                and id2uid_score * exp(-0.1) > exp(-18) and uid2id_score * exp(-0.1) > exp(-18)
        UNION ALL
            select transform (t2.relobj_id,concat_ws('|,|',collect_set(concat(t2.mix_uid,'|:|',t2.weight,'|:|',t2.uid2id_score))),t2.user_type,t2.relobj_type)
            using 'python cal_iduid_score.py'
            as (mix_uid,relobj_id,id2uid_score,uid2id_score,user_type,relobj_type)
            from (
                select transform (mix_uid,concat_ws('|,|',collect_set(concat(relobj_id,'|:|',weight,'|:|'))),user_type,relobj_type)
                using 'python cal_uidid_score.py'
                as (mix_uid,relobj_id,weight,uid2id_score,user_type,relobj_type)
                from(
                    select mix_uid, relobj_id, weight, user_type, relobj_type
                    from %(source_tb)s
                    where day_id = %(ARG_OPTIME)s
                        and provider = '%(provider)s' and province = '%(province)s'
                        and net_type='%(NET_TYPE)s' and mix_uid!='' and relobj_id != ''
                ) t1
                group by mix_uid,user_type,relobj_type
            ) t2
            where t2.uid2id_score >= 0.05
            group by t2.relobj_id,t2.user_type,t2.relobj_type
        ) t3
        group by mix_uid, relobj_id, user_type, relobj_type
    ''' % vars())
    HiveExe(hivesql, name, dates)

 #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)