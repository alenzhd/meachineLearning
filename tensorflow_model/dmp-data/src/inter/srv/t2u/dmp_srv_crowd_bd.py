#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：dmp_um_id_rel_bd.py
# **  功能描述：
# **  特殊说明：
# **  输入表：  
# **          
# **  调用格式：   dmp_um_id_rel_bd.py provider province net_type date ip
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

dates = sys.argv[4]
queue = QUEUE
if(queue == ''):
    queue='default'

#hadoop相关参数
hadoop_params=["/user/gdpi/public/sada_gdpi_click.password=GWDPI-SH",
               "/user/gdpi/public/sada_gdpi_adcookie.password=CKDPI-SH",
               "mapreduce.input.fileinputformat.split.maxsize=100000000",
               "mapreduce.input.fileinputformat.split.minsize.per.node=100000000",
               "mapreduce.input.fileinputformat.split.minsize.per.rack=100000000",
               "hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat",
               "mapreduce.map.memory.mb=2048",
               "mapreduce.reduce.memory.mb=5120",
               "mapreduce.job.queuename="+queue
               ]

try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']

    source_table = "dmp_ud_id_tags_bd"
    target_table = "dmp_srv_crowd_bd"

#==========================================================================================
#建表
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(target_table)s (
            flag_id string
        )
        partitioned by (day_id string, owner string, type_id string, flag_type string, id string)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
        location '%(HIVE_TB_HOME)s/%(target_table)s';
    '''% vars())
    HiveExe(hivesql,name,dates)

#==========================================================================================
#===========================================================================================
    args0 = 'crowd,'+dates
    args1 = HIVE_TB_HOME + "," + HIVE_DATABASE

    #拼接hadoop参数
    args2 = ""
    for i in range(len(hadoop_params)):
        args2+=hadoop_params[i]+","
    if(args2 != ""):
        args2 = args2[:-1]

    cmd = "hadoop jar " + JAR_DMP_SRV + " " + args0 + " " + args1
    if(args2 != ""):
        cmd += " " + args2
    print cmd
    os.system(cmd)
#===========================================================================================
    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
