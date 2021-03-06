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
HIVE_DATABASE = 'dmp_yx'
#HIVE数据存储根目录
HIVE_TB_HOME='/DMP_YX/hive/warehouse/dmp_yx.db/dmp3/'

#hadoop资源队列
QUEUE = 'DMP_YX'

#内容识别参数,可以为空;多个参数采用分好隔开
DMP_CI_CONFIG_PARAMS=''

##hive默认文件格式
HIVE_DEFAULT_FILE_TYPE='ORC'

##定义分隔符
DDM_FS = '\t'
