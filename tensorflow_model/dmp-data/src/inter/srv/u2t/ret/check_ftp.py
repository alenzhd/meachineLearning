#!/usr/bin/env python
# -*-coding:utf-8 -*-
import os,sys,time
import urllib2
import json

reload(sys)
sys.setdefaultencoding('utf-8')

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
dates=sys.argv[4]
flag=sys.argv[5]
module=sys.argv[6]

#===========================================================================================
url='http://10.1.3.203:9099/api/queryftp?module='+module+'&provider='+provider+'&province='+province+'&flag='+flag+'&dayid='+dates
print url
response = urllib2.urlopen(url)
codes=json.loads(response.read())
code = codes['code']
message=codes['message']
if(code == '1'):
    print "检查通过:"+code+","+message
    sys.exit(0)
else:
    print "异常:"+message
    sys.exit(-1)
