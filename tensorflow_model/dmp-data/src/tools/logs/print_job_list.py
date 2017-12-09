#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：xxxxxxxxx
# **  创建者:   xxx
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
import os,sys,time
#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../../'
python_path = []
python_path.append(mix_home+'/conf')
sys.path[:0]=python_path

 
#引入自定义包
from settings import *
logdir=sys.argv[4]

dates="19700101"

#程序开始执行
name =  sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
try:
#传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print dicts['ARG_OPTIME']
#===========================================================================================
    while (1):
        job=os.popen("hadoop job -list|grep vendoryx")
        ss=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))+"\n"+job.read()
        yyyymmdd=time.strftime('%Y%m%d',time.localtime(time.time()))
        if os.path.exists(logdir+'/'+yyyymmdd) != True :
		    os.makedirs(logdir+'/'+yyyymmdd)
        f = open(logdir+'/'+yyyymmdd+'/job_log_'+yyyymmdd+'.log','a')
        f.write(ss)
        f.close()
        time.sleep(60)

#程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
