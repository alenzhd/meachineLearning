#!/usr/bin/env python
# -*-coding:utf-8 -*-
#monitor_holiday.py
#*******************************************************************************************
# **  文件名称:schedule.py
# **  功能描述：
# **           
# **  特殊说明： 通知用户管理的接口：采用redis的方式。
# **      Key格式：NOTICE_UM_provider_province_netType_date
# **  例如：NOTICE_UM_dxy_beijing_mobile_20160311
# **      Value：固定值15
# **          
# **  调用格式：   python schedule.py $provider $province $net_type $day 
# **  输出表： 
# **           
# **  创建者:   guojy
# **  创建日期: 2016/03/16
# **  修改日志: **  修改日期: 修改人: 修改内容:
#********************************************************************************************
# **  Copyright(c) 2013 AsiaInfo Technologies (China), Inc. 
# **  All Rights Reserved.
#********************************************************************************************
import os,sys
import datetime
import time
import redis
#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../../'
python_path = []
python_path.append(mix_home+'/conf')
sys.path[:0]=python_path

#引入自定义包
from settings import *

#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')

dates = sys.argv[4]


try:    
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print dicts['ARG_OPTIME']
    
    
    while True:
        #r = redis.Redis(host='10.1.1.100',port=6399,db=0)
        r = redis.Redis(host=redis_host,port=redis_port,db=0)
#====================================================
# **  例如：NOTICE_UM_dxy_beijing_mobile_20160311
# **      Value：固定值15
# 测试：r.set('NOTICE_UM_dxy_beijing_mobile_20160311','15') 
#       redis_get_value=r.get('dxy_hubei_a_20160311') value值为15
#=====================================================================
        key = "NOTICE_UM_"+PROVIDER+"_"+PROVINCE+"_"+NET_TYPE+"_"+dates
        redis_get_value=str(r.get(key))
        if redis_get_value == '15':
            #r.delete(key)
            break
        else:
            #休眠五分钟
            time.sleep(300) 
            continue

#程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)