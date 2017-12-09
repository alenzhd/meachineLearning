#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：del_table.py
# **  功能描述：删除过期的分区
# **  创建者:   hlx
# **  创建日期: 2016/01/19
# **  修改日志:
# **  修改日期: yyyy/mm/dd，修改人 xxx  ，修改内容：流程简化
#********************************************************************************************
import os,sys,time,datetime
#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../../'
python_path = []
python_path.append(mix_home+'/conf')
sys.path[:0]=python_path

 
#引入自定义包
from settings import *
from dim_dmp_conf import *

def get_date_of_back_someday(day_id,time_length):
    format="%Y%m%d"
    t = time.strptime(day_id, "%Y%m%d")
    result=datetime.datetime(*time.strptime(str(day_id),format)[:6])-datetime.timedelta(days=int(time_length))
    return result.strftime(format)

def delPath(path):
    flag = False
    cmd = "hdfs dfs -ls "+path
    output = os.popen(cmd)
    output_lines = output.readlines()
    if output_lines:
        flag = True
        for line in output_lines:
            idx = line.find("day_id=")
            if idx != -1 and len(line) >= idx + 15:
                dayid = line[idx+7:idx+15]
                if dayid <= date_1:
                    delCmd = "hdfs dfs -rm -r -skipTrash " + path + '/day_id='+dayid
                    print delCmd
                    status = os.system(delCmd)
                    if status == 0 :
                        print "OK!"
                    else:
                        print "WARN!"
    # else:
    #     print "no path warn!path="+path
    return flag

#程序开始执行
name =  sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
dates = sys.argv[4]
try:
#传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']

    #处理dim_dmp_conf['DMP_CONF_TABLE_RETAIN_DAYS']
    tableNames=dim_dmp_conf['DMP_CONF_TABLE_RETAIN_DAYS'].split(';')
    # tableNames = ['dmp_ci_url_m_bh=1']
    for i in range(len(tableNames)):
        kv = tableNames[i].split('=')
        if len(kv)==2:
            date_1=get_date_of_back_someday(dates,kv[1])
            talbename = kv[0]
            print "删除表"+talbename+","+date_1+"及以前的数据!"
    
            path = HIVE_TB_HOME + talbename + '/provider='+PROVIDER+'/province='+PROVINCE+'/net_type='+NET_TYPE
            flag = delPath(path)
            if not flag:
                path = HIVE_TB_HOME + talbename + '/provider='+PROVIDER+'/province='+PROVINCE
                delPath(path)
        else:
            print 'Error tablename: ',tableNames[i]

#程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)

