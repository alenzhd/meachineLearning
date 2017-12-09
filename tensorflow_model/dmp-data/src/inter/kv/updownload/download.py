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
mix_home=current_path+'/../../../../'
python_path = []
python_path.append(mix_home+'conf')
sys.path[:0]=python_path
print sys.path

#引入自定义包
from settings import *

#程序开始执行

province = PROVINCE
provider = PROVIDER
nettype = NET_TYPE
dayhourId = sys.argv[3]
tablename = sys.argv[4] #表名

name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')+"_"+tablename
dates = dayhourId
try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']

    args0 =  provider + "," + province + "," + nettype + "," + dayhourId
    args1 = "download,"+tablename
    args2 = HIVE_TB_HOME + "," + HIVE_DATABASE +","+mix_home

    cmd = "java -jar "+current_path+"/../../../../lib/dmp-updownload.jar " + args0 + " " + args1
    if(args2 != ""):
        cmd += " " + args2
    print cmd
    os.system(cmd)

    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
