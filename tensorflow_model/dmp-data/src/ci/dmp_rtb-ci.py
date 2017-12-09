#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：xxxxxxxxx
# **  输入表：  dmp_rtb-ci_bh
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

dayId = sys.argv[4]  #天
queue = QUEUE

queue = ''
if(queue == ''):
    queue='default'

#hadoop相关参数
hadoop_params=["mapreduce.input.fileinputformat.split.maxsize=150000000",
               "mapreduce.input.fileinputformat.split.minsize.per.node=150000000",
               "mapreduce.input.fileinputformat.split.minsize.per.rack=150000000",
               "hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat",
               "mapreduce.map.memory.mb=2048",
               "mapreduce.reduce.memory.mb=5120",
               "mapreduce.job.queuename="+queue
]

print hadoop_params
dates = dayId[:8]
try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']

    args0 =  dayId
    args1 = HIVE_TB_HOME + "," + HIVE_DATABASE

    #拼接hadoop参数
    args2 = ""
    for i in range(len(hadoop_params)):
        args2+=hadoop_params[i]+","
    if(args2 != ""):
        args2 = args2[:-1]

    cmd = "hadoop jar " + JAR_DMP_RTB_CI + " " + args0 + " " + args1
    if(args2 != ""):
        cmd += " " + args2
    print cmd
    os.system(cmd)

    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
