#!/usr/bin/env python
# -*-coding:utf-8 -*-
import os,sys,subprocess
import time,random

from settings import *

v_current_path=os.path.dirname(os.path.abspath(__file__))
v_mix_home=v_current_path+'/../'
v_python_path = []
sys.path[:0]=v_python_path

filepath=DIM_HOME+'/dim_'+NET_TYPE+'/dim_dmp_conf/dim_dmp_conf'

#dim_dmp_conf
dim_dmp_conf={}

flag=False

dim_provinces={}
cat = subprocess.Popen(['hadoop', 'fs', '-text', filepath],stdout=subprocess.PIPE)

try:
    for line in cat.stdout:
        s=line.strip().split("\t")
        if len(s) == 2:
            dim_dmp_conf[s[1]]=''
            print s[1]+":"
        else:
            dim_dmp_conf[s[1]]=s[2]
            print s[1]+":"+dim_dmp_conf[s[1]]
    if v_provider=='dxy':
        prvinces = open(v_mix_home+'/conf/'+v_switch+'/'+v_provider+'/common/'+v_nettype+'/provinces','w')
        ps=dim_dmp_conf['DMP_CONF_'+NET_TYPE.upper()+'_PROVINCES_BD']
        prvinces.writelines(ps)
        prvinces.close()
        for p in ps.strip().split("#"):
            a=p.split("=")[0]
            b=p.split("=")[1]
            dim_provinces[a]=b
        i=dim_provinces[PROVINCE].split("%")[0]
        n=dim_provinces[PROVINCE].split("%")[1]
        for line in os.popen("date --date="+YYYYMMDD+" +%s"):
            if int(line.strip())/3600/24%int(n) == int(i):
                print "yes-run-today:"+v_provider+"-"+v_province+"-"+v_nettype+"-"+v_yyyymmdd
            else:
                print "no-run-today:"+v_provider+"-"+v_province+"-"+v_nettype+"-"+v_yyyymmdd
                if v_yyyymmdd[-2:] == '01':
                    print "run every day 01"
                    flag=True
                    time.sleep(random.randint(100,1000))
                else:
                    sys.exit(-1)
					
    #打印dmp-data工程版本信息
    fr = open(v_mix_home + 'conf/VERSION')
    for line in fr.readlines():
        print 'dmp-data:' + line
except Exception,e:
    print "ignore-exception"
    print e

