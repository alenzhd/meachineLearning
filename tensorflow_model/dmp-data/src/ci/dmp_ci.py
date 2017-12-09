#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：xxxxxxxxx
# **  输入表：  dmp_ci_bh
# **  输出表:   dmp_table
# **  创建者:   fengbo
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
startHourId = sys.argv[4]  #开始时间
endHourId = sys.argv[5]  #结束时间
JOBNAME = sys.argv[6]  #需要执行的job，值为job1,job2
queue = QUEUE
if(queue == ''):
    queue='default'

#hadoop相关参数
hadoop_params=["mapreduce.input.fileinputformat.split.maxsize=157286400",
               "mapreduce.input.fileinputformat.split.minsize=157286400",
               "mapreduce.input.fileinputformat.split.minsize.per.node=157286400",
               "mapreduce.input.fileinputformat.split.minsize.per.rack=157286400",
               "hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat",
               "mapreduce.map.memory.mb=2048",
			   "mapreduce.map.failures.maxpercent=50",
			   "mapreduce.reduce.failures.maxpercent=50",
               "mapreduce.job.queuename="+queue
]

print hadoop_params

dates = startHourId[:10]
try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']

    args0 =  PROVIDER + "," + PROVINCE + "," + NET_TYPE + "," + startHourId + "," + endHourId
    args1 = HIVE_TB_HOME + "," + HIVE_DATABASE + "," + JOBNAME + "," + DIM_HOME

    #拼接hadoop参数
    args2 = ""
    for i in range(len(hadoop_params)):
        args2+=hadoop_params[i]+","
    if(args2 != ""):
        args2 = args2[:-1]

    args3 = ""
    if(DMP_CI_CONFIG_PARAMS != ""):
        args3 = "config:"+DMP_CI_CONFIG_PARAMS

    cmd = "hadoop jar " + JAR_DMP_CI + " " + args0 + " " + args1
    if(args2 != ""):
        cmd += " " + args2
    if(args3 != ""):
        cmd += " " + args3
    print cmd
    os.system(cmd)

    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
