#!/usr/bin/env python
# -*-coding:utf-8 -*-

import os,sys
import datetime
import time
import redis

#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../../../../'
python_path = []
python_path.append(mix_home+'conf')
sys.path[:0]=python_path
print sys.path

#引入自定义包
from settings import *

provider=PROVIDER
province=PROVINCE
net_type=NET_TYPE
dates = sys.argv[4]
process= sys.argv[5]

providers={"dxy":"dxy","lt":"lt","yd":"yd"}
processes={"1":"d","2":"p","3":"u","u2t":"u2t","u2a":"u2a"}
states={"start":"s","end":"e"}
nettypes={"adsl":"a","mobile":"m"}
provinces={"beijing":"11","tianjin":"12","hebei":"13","shanxi":"14","neimenggu":"15",
"liaoning":"21","jilin":"22","heilongjiang":"23","shanghai":"31","jiangsu":"32",
"zhejiang":"33","anhui":"34","fujian":"35","jiangxi":"36","shandong":"37","henan":"41",
"hubei":"42","hunan":"43","guangdong":"44","guangxi":"45","hainan":"46","chongqing":"50",
"sichuan":"51","guizhou":"52","yunnan":"53","xizang":"54","shanxisheng":"61","gansu":"62",
"qinghai":"63","ningxia":"64","xinjiang":"65"}



while True:
        r = redis.Redis(host='10.1.1.100',port=6399,db=0)
        list = r.keys(providers[provider]+"_"+processes[process]+"_e_"+nettypes[net_type]+"_"+provinces[province]+"_"+dates)
        if  len(list) >= 1:
            #r.delete(key)
            print "已完成:"+str(len(list))
            sys.stdout.flush()
            break
        else:
            print "未完成:"+str(len(list))
            sys.stdout.flush()
            #休眠五分钟
            time.sleep(10) 
            continue

