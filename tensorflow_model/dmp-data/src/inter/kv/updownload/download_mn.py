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

tablename = sys.argv[3] #表名
dayhourId = 0

name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')+"_"+tablename

cur_datetime = datetime.datetime.now()
cur_datetime_Last3Hour = cur_datetime + datetime.timedelta(hours=-4)
cur_datetime_Last3Hour = cur_datetime_Last3Hour.strftime('%Y%m%d%H');

cur_datetime_Last1Day = cur_datetime + datetime.timedelta(days=-1)
cur_datetime_Last1Day=cur_datetime_Last1Day.strftime('%Y%m%d');

if(tablename == 'dmp_mn_kpi_bh'):
    dayhourId = cur_datetime_Last3Hour
elif(tablename == 'dmp_mn_kpi_bd'):
    dayhourId = cur_datetime_Last1Day
else:
    print "tablename is error！"
    sys.exit()

dates = dayhourId
try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']

    cmd = "python "+current_path+"/download.py "+dayhourId+" " +tablename 
    print cmd
    status = os.system(cmd)

    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
