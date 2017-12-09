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

#测试区和生产区切换开关：test/online
v_switch = 'online'

#参数传入
v_provider =  sys.argv[1]
v_province =  sys.argv[2]
v_nettype  =  sys.argv[3]
v_yyyymmdd =  sys.argv[4]
#运营商: yd/lt/dx/dxy
PROVIDER = v_provider
#省份：zhejiang/shanghai/jiangsu/...
PROVINCE = v_province
#数据类型：mobile/adsl
NET_TYPE = v_nettype
#数据日期
YYYYMMDD = v_yyyymmdd[0:8]

v_current_path=os.path.dirname(os.path.abspath(__file__))
v_mix_home=v_current_path+'/../'
v_python_path = []

python_cmd ='python'
if v_provider=='dxy':
    python_cmd = '/usr/bin/python'
    v_python_path.append(v_mix_home+'/conf/'+v_switch+'/'+v_provider+'/common/'+v_nettype)
    sys.path[:0]=v_python_path
else :
    v_python_path.append(v_mix_home+'/conf/'+v_switch+'/'+v_provider+'/'+v_province+'/'+v_nettype)
    sys.path[:0]=v_python_path

#引入自定义包
from hqltool import *
from dim_dmp_conf import *

########################以下部分配置一般不用修改##############################
mix_conf = os.path.dirname(os.path.abspath(__file__))
mix_home = mix_conf +'/../'
MIX_LIB_PATH = mix_home+'./lib'
## jar包路径
JAR_DMP_UL=MIX_LIB_PATH + '/dmp-ul-0.0.1-SNAPSHOT.jar'
JAR_DMP_CI = MIX_LIB_PATH + '/dmp-ci-0.0.1-SNAPSHOT.jar'
JAR_MIX = MIX_LIB_PATH + '/mix-dmp-udf-1.0.0.jar'
JAR_DMP_UC = MIX_LIB_PATH + '/dmp-uc.jar'
JAR_DMP_SRV = MIX_LIB_PATH + '/dmp-srv.jar'
JAR_DMP_RTB_CI = MIX_LIB_PATH + '/dmp-rtb-ci.jar'


# 测试环境上传数据服务IP和端口(IDC)
#UPLOAD_IP_PORT = '10.1.3.91:18090'
# 正式环境上传数据服务IP和端口(IDC)
UPLOAD_IP_PORT = '10.1.3.127:18090'

#ud标签上传url,生产环境
#ud_label_url='http://10.1.3.127:18090/behavior/save?version=1.2#http://10.1.3.128:18090/behavior/save?version=1.2#http://10.1.3.129:18090/behavior/save?version=1.2'
ud_label_url='http://10.1.2.120:18090/behavior/save?version=1.2'
#ud标签上传url,测试环境
#ud_label_url='http://10.1.3.91:18090/behavior/save?version=1.2'

#srv_adrel数据上传url，生产环境
#srv_adrel_url='http://10.1.3.127:18090/usertracking/ad/save?version=1.0#http://10.1.3.128:18090/usertracking/ad/save?version=1.0#http://10.1.3.129:18090/usertracking/ad/save?version=1.0'
srv_adrel_url='http://10.1.2.120:18090/usertracking/ad/save?version=1.0'
#srv_adrel数据上传url，测试环境
#srv_adrel_url='http://10.1.3.91:18090/usertracking/ad/save?version=1.0'

#srv_adrel数据上传url，测试环境
#srv_adrel_url='http://10.1.3.91:18090/usertracking/ad/save?version=1.0'

#idmapping数据上传url，测试环境
#srv_idmapping_url='http://10.1.3.91:18090/common/save'
#idmapping数据上传url，生产环境
srv_idmapping_url='http://10.1.2.120:18090/common/save'

