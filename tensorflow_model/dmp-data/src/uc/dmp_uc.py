#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：用户汇总上传
# **  创建者:   luozs
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

#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
dates = sys.argv[4]  #天日期
action = sys.argv[5]  #执行计划,"IRU"
queue = QUEUE
if(queue == ''):
    queue='default'

#hadoop相关参数
hadoop_params=["mapreduce.input.fileinputformat.split.maxsize=157286400",
               "mapreduce.input.fileinputformat.split.minsize=157286400",
               "mapreduce.input.fileinputformat.split.minsize.per.node=157286400",
               "mapreduce.input.fileinputformat.split.minsize.per.rack=157286400",
               "hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat",
			   "mapreduce.map.failures.maxpercent=10",
			   "mapreduce.reduce.failures.maxpercent=10",
               "mapreduce.job.queuename="+queue,
               "dxy.upload.ip_host="+host_port,
               "if.create.tables="+UC_IF_CREATE_TABLES
]

print hadoop_params

try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name+action,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']
    if(PROVIDER != 'dxy'):
        DXY_TB_HOME=HIVE_TB_HOME

    args0 =  PROVIDER + "," + PROVINCE + "," + NET_TYPE + "," + dates
    args1 = HIVE_TB_HOME + "," + HIVE_DATABASE + "," + DIM_HOME
    args2 = action

    #拼接hadoop参数
    args3 = ""
    for i in range(len(hadoop_params)):
        args3+=hadoop_params[i]+","
    if(args3 != ""):
        args3 = args3[:-1]

    cmd = "hadoop jar " + JAR_DMP_UC + " " + args0 + " " + args1 + " " + args2
    if(args3 != ""):
        cmd += " " + args3
    print cmd
    os.system(cmd)

    #程序结束
    End(name+action,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
