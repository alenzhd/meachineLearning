#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：xxxxxxxxx
# **  输入表：  
# **  输出表:   
# **  创建者:   luozs
# **  创建日期: yyyy/mm/dd
# **  修改日志:
# **  修改日期: yyyy/mm/dd，修改人 xxx  ，修改内容：流程简化
# ** ---------------------------------------------------------------------------------------
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
sys.stdout.flush()

#引入自定义包
from settings import *

#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
province = PROVINCE
provider = PROVIDER
net_type = NET_TYPE
dates = sys.argv[4]

hive_home = HIVE_TB_HOME[:-1]
#hive_home = hive_home[:-1]
hive_database = HIVE_DATABASE
url = ''
table_name = sys.argv[5]

queue = QUEUE
if(queue == ''):
    queue='default'
if( table_name == 'dmp_srv_id_htags_bd'):
    url = ud_label_url
elif( table_name == 'dmp_srv_adrel_bd'):
    url = srv_adrel_url
else:
    url = srv_idmapping_url

## 需要上传的数据路径
table_data_path=HIVE_TB_HOME + table_name +'/day_id=' + dates
#输出需要的参数
print provider
print province
print net_type
print dates

print hive_home
print hive_database
print url
print table_name
print table_data_path
sys.stdout.flush()


#hadoop相关参数
hadoop_params=["mapreduce.input.fileinputformat.split.maxsize=50000000",
               "mapreduce.input.fileinputformat.split.minsize.per.node=50000000",
               "mapreduce.input.fileinputformat.split.minsize.per.rack=50000000",
               "mapreduce.map.memory.mb=900",
               "mapreduce.job.queuename="+queue
]

print hadoop_params
try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']

    args0 =  PROVIDER + "," + PROVINCE + "," + NET_TYPE + "," + dates
    args1 = hive_home + "," + HIVE_DATABASE + "," + url + "," + table_name +',' + table_data_path

    #拼接hadoop参数
    args2 = ""
    for i in range(len(hadoop_params)):
        args2+=hadoop_params[i]+","
    if(args2 != ""):
        args2 = args2[:-1]

    cmd = "hadoop jar " + JAR_DMP_SRV + " " + args0 + " " + args1
    if(args2 != ""):
        cmd += " " + args2
    print cmd
    print "开始上传idmapping标签！"
    sys.stdout.flush()
    os.system(cmd)

    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
