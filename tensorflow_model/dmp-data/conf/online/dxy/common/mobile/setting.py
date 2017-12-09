#!/usr/bin/python
# -*-coding:utf-8 -*-
#***************************************************************************************************
# **  文件名称:       settings.py
# **  功能描述:       共用变量定义文件,文件中定义的变量可能会被多个文件、模块使用所以修改时请慎重
# **  创建者：        刘加林
# **  创建日期        2013年8月21
# **  修改日志
# **  修改日期
# ** -----------------------------------------------------------------------------------------------
# **
# **  Copyright(c) 2007 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
#***************************************************************************************************

#***************************************************************************************************
# ** 常用变量、系统维护变量
#***************************************************************************************************

import os,sys

#HIVE数据库名
HIVE_DATABASE = 'vendoryx'
#HIVE数据存储根目录
HIVE_TB_HOME='/user/vendoryx/'
#规则目录
DIM_HOME='/user/vendoryx/'
#hadoop资源队列
QUEUE = 'root.vendor.ven4'

#redis 通知接口
redis_host = "10.1.1.100"
redis_port = 6399

#电信云set接口ip和端口
#host_port = "10.0.171.162:8080"
host_port = "10.0.174.85:8082"
print "电信云set接口ip和端口："+host_port

#内容识别参数,可以为空;多个参数采用#隔开
DMP_CI_CONFIG_PARAMS='data.input.src.path=/daas/bstl/dpiqixin#if.create.tables=false#is.output.these.tables=false'

#用户汇总是否创建表
UC_IF_CREATE_TABLES='false'

##hive默认文件格式
HIVE_DEFAULT_FILE_TYPE='ORC'

##定义分隔符
DDM_FS = '\t'
