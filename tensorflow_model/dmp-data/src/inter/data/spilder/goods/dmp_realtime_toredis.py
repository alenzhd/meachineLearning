#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：dmp_realtime_toredis.py
# **  功能描述：监控爬虫结果目录output，商品信息爬取完成后，入实时商品信息表
# **            并整理为key-value格式上传到redis和商品库
# **  特殊说明： 
# **  输入表：  
# **          
# **  调用格式：   python dmp_realtime_toredis.py $yyyyMMdd
# **                     
# **  输出表： 
# **           
# **  创建者:   guojy
# **  创建日期: 2015/09/28
# **  修改日志:
# **  修改日期: 2015/10/29 修改人: zhangqn 修改内容:优先上传redis, 该脚本中不对hive库进行处理
# ** ---------------------------------------------------------------------------------------
# **  
# ** ---------------------------------------------------------------------------------------
# **  
# **    
#********************************************************************************************
# **  Copyright(c) 2013 AsiaInfo Technologies (China), Inc. 
# **  All Rights Reserved.
#********************************************************************************************
import os,sys
import datetime
import time
import redis
import os.path
import json
reload(sys)
sys.setdefaultencoding('utf8')

#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../../../../'
tmpdata_path=mix_home+'/tmp/'
python_path = []
python_path.append(mix_home+'/conf')
sys.path[:0]=python_path
print sys.path
#引入自定义包
from settings import *

remote_path= '/data/ftp/ai-spider/dmp-filter/input_rt/'
output_path = '/data/ftp/ai-spider/dmp-filter/output_rt/'
#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
dates = time.strftime('%Y%m%d',time.localtime(time.time()))

def add_write(key,value):
    try:
        reader = open(current_path+"/realtime_conf.properties")
        conf = dict()
        for line in reader:
            temp = line.strip().split(":")
            conf[temp[0]] = temp[1]
        #连接redis
        r = redis.Redis(host=conf["host"], port=conf["port"], db=0)
        #上传数据到redis
        r.set(key,value)
        reader.close()
    except Exception, exception:
        print exception

#==========================================================================================
#目标表创建
#=============================================================================    
try:
    Start(name,dates)
    while True:
#==========================================================================================
#监控爬虫结果：
#每两分钟扫描output目录下的文件，将新增文件数据load到维表
#输入输出文件格式样例：20150925-000000-item.txt.final (年月日-时分秒.txt.final)
#filename=str(dates[0:8])+'-'+dates[8:10]+'0000-item.txt.final'
#=============================================================================
        #备份爬取结果文件
        outputbak = tmpdata_path + '/goods/bak_output_realtime/'
        filename =''
        ask = 0
        while True:
            files = os.listdir(output_path)
            if  len(files)!=0:
                print "执行加载文件操作"
                for file in files:
                    if os.path.splitext(file)[1] == '.final':
                        filename = file
                        print "即将加载文件为："+filename+"，准备上传redis-----"
                        target_file = output_path +"/"+filename
#===========================================================================================
#上传到redis
#=============================================================================
                        print "=====开始上传商品信息到redis======"
                        lines = open(target_file)
                        for line in lines:
                            try:
                                line=line.replace("\\t"," ").strip()
                                col=json.loads(line)
                                goods_id=str(col["itemCode"])
                                site_id=str(col["mpWebsitId"])
                                site_cate_id=str(col["mpClassCode"])
                                if("mpClassName" in col):
                                    site_cate_name=col["mpClassName"]
                                else:
                                    site_cate_name=''
                                title=col["title"]
                                price=col["price"]
                                if("brandCode" in col):
                                    brand_code=col["brandCode"]
                                else:
                                    brand_code=''
                                if("brandName" in col):
                                    brand_code=col["brandName"]
                                else:
                                    brand_name=''
                                if("stdClassCode" in col):
                                    std_cate_id=str(col["stdClassCode"])
                                else:
                                    std_cate_id=''
                                if("stdClassName" in col):
                                    std_cate_name=col["stdClassName"]
                                else:
                                    std_cate_name=''
                                update_date=str(col["collectDate"])
                                site=goods_id.split('-')[0]
                                if goods_id!='' and site_cate_id!='' and price!='' and title!='' and site_cate_id.find('http')<0:
                                    key = site_id+'_'+goods_id.split('-')[1]
                                    val = '{"goods_id":"'+goods_id.split('-')[1]+'","site_id":"'+site_id+'","site_cate_id":"'+site_cate_id+'","site_cate_name":"'+site_cate_name+'","title":"'+title+'","price":"'+price+'","std_cate_id":"'+std_cate_id+'","std_cate_name":"'+std_cate_name+'","update_date":"'+update_date+'"}'
                                    add_write(key,val)
                            except Exception, err:
                                continue
                        print filename+"中的商品信息上传redis成功！"
                        lines.close()
                        if not os.path.isdir(outputbak+filename.split('-')[0]+"/"):
                            os.makedirs(outputbak+filename.split('-')[0]+"/")
                        os.system("mv "+target_file+" "+outputbak+filename.split('-')[0]+"/")
                if filename!='':
                    break
                ask+=1
                print ">>>>>没有需要加载的文件，二分钟后会进行第"+str(ask)+"次尝试>>>>>"
                time.sleep(120)

            else:
                ask+=1
                print ">>>>>输出文件未生成，二分钟后会进行第"+str(ask)+"次尝试>>>>>"
                time.sleep(120)

        #休眠五分钟
        time.sleep(300)

    #===========================================================================================

    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)

