#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：dmp_realtime_getredis.py
# **  功能描述：前端将未查询到的商品ID插入redis（所有商品ID需要添加前缀：REAL_）, 
# **            后端每5分钟回去遍历REAL_的所有key进行爬取，
# **            将未爬取的商品信息从redis读取到文件到爬虫目录input_realtime
# **           
# **  特殊说明： 
# **  输入表：  
# **          
# **  调用格式：   python dmp_realtime_getredis.py $yyyyMMdd
# **                     
# **  输出表： 
# **           
# **  创建者:   guojy
# **  创建日期: 2015/09/28
# **  修改日志:
# **  修改日期: 修改人: 修改内容:
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
#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../../../../'
tmpdata_path=mix_home+'/tmp/'
python_path = []
python_path.append(mix_home+'/conf')
sys.path[:0]=python_path
print sys.path


remote_path= '/data/ftp/ai-spider/dmp-filter/input_rt/'
output_path = '/data/ftp/ai-spider/dmp-filter/output_rt/'   


#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')


#===========================================================================================
#每五分钟连接一次redis。并上传redis中没有爬取过的商品ID到爬虫input文件夹
#输入输出文件格式样例：20150925-000000-item.txt.final (年月日-时分秒.txt.final)
#===========================================================================================

try:
    while True:
        inputdir = tmpdata_path + '/goods/input_realtime/'
        if not os.path.isdir(inputdir):  
            os.makedirs(inputdir)     
        #获取当前时间，精确到分
        dates=time.strftime('%Y%m%d-%H%M00',time.localtime(time.time()))          
        filename= dates + '-item.txt.final'
        inputfile = tmpdata_path + '/goods/input_realtime/' + filename 
    
        
        reader = open(current_path+"/realtime_conf.properties")
        conf = dict()
        for line in reader:
            temp = line.strip().split(":")
            conf[temp[0]] = temp[1]
        #连接redis
        r = redis.Redis(host=conf["host"], port=conf["port"], db=0)
        
        #判断是否有REAL_开头的商品数据，并读取key值，key: REAL_8_1194245234 ,
        #site_id对应的网站:8_jd, 7_taobao , 9_tmall, 30_suning,13_meituan,329_nuomi
        if len(r.keys('REAL_*'))!=0:
            writer = open(inputfile,'w')
            keys = r.keys("REAL_*")
            for k in keys:
                sp = k.split("_")
                site_id = sp[1]
                if (site_id == '7'):
                    site_name = 'taobao'
                if (site_id=='8'):
                    site_name = 'jd'
                if (site_id == '9'):
                    site_name = 'tmall'
                if (site_id == '30'):
                    site_name = 'suning' 
                if (site_id == '32'):
                    site_name = 'yhd' 
                if (site_id == '34'):
                    site_name = 'jumei' 
                if (site_id == '40'):
                    site_name = 'dangdang'
                if (site_id == '13'):
                    site_name = 'meituan'
                if (site_id == '329'):
                    site_name = 'nuomi'          
                goodsid = '_'.join(str(i) for i in sp[2:]) 
                key = site_name+"-"+goodsid
                writer.write(key+'\n')
            reader.close()
            writer.close()
            #上传增量的商品ID文件
            os.system("cp "+inputfile+" "+remote_path)
            print "上传文件"+filename+"成功！" 
        else:
            print "当前时间"+dates+"，没有需要爬取的实时商品ID，五分钟后再次爬取！"
            reader.close()
            #休眠五分钟
            time.sleep(300) 
            continue
        
        #删除已经爬取的商品ID, site_id对应的网站:8_jd, 7_taobao , 9_tmall, 30_suning
        reader_goods = open(inputfile)
        for lt in reader_goods:
            line = lt.strip().split("-")
            st_name = line[0]
            goods_id = line[1]
            if (st_name == 'taobao'):
                st_id ='7'
            if (st_name == 'jd'):
                st_id ='8'
            if (st_name == 'tmall'):
                st_id ='9'
            if (st_name == 'suning'):
                st_id ='30'
            if (st_name == 'yhd'):
                st_id ='32'
            if (st_name == 'jumei'):
                st_id ='34'
            if (st_name == 'dangdang'):
                st_id ='40'
            if (st_name == 'meituan'):
                st_id = '13'
            if (st_name == 'nuomi'):
                st_id = '329'
            tmp = 'REAL_'+st_id+'_'+goods_id
            print tmp
            d = r.delete(tmp)
        print "redis中已经爬取的商品ID，删除完成！"
        reader_goods.close()
        
        #休眠五分钟
        time.sleep(300) 
    
#异常处理
except Exception,e:
    print e
