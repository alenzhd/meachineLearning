#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：dmp_spider_goods_put2srv.py
# **  功能描述：上传与商品信息表未匹配上的商品ID到爬虫input目录，监控爬虫结果目录output，
# **            商品信息爬取完成后，入dim维表，过滤dim表中的不可用数据插入到商品信息表。
# **            商品信息入库。整理为key-value格式
# **  特殊说明：
# **  输入表：  
# **          
# **  调用格式：   dmp_spider_goods_put2srv.py $yyyyMMdd
# **                     
# **  输出表： 
# **           
# **  创建者:   ymh
# **  创建日期: 2015/08/26
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
#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../../../'
tmpdata_path=mix_home+'/tmp/'
python_path = []
python_path.append(mix_home+'/conf')
sys.path[:0]=python_path
print sys.path
#引入自定义包
from settings import *

def get_date_of_back_someday(day_id,time_length):
    format="%Y%m%d"
    t = time.strptime(day_id, "%Y%m%d")
    result=datetime.datetime(*time.strptime(str(day_id),format)[:6])-datetime.timedelta(days=int(time_length))
    return result.strftime(format)   
#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
dates = sys.argv[3]
before3days=get_date_of_back_someday(dates,3)
before30days=get_date_of_back_someday(dates,30)
try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']
    
    dmp_spider_goods_bd= "dmp_data_goods_base_bd"
    dmp_spider_goods= "dmp_data_goods_base"
    dmp_spider_toredis_bd= "dmp_srv_toredis_bd"
#==========================================================================================
#创建目标表dmp_user_address_score
#===========================================================================================
 
    
    hivesql = []
    hivesql.append('''
        create table if not exists %(dmp_spider_toredis_bd)s ( 
            key    string,
            value    string
        ) partitioned by (day_id string,type string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(dmp_spider_toredis_bd)s'
    '''% vars())
    HiveExe(hivesql,name,dates)
#===========================================================================================
#清洗后数据入库到商品信息表，forredis表
#=============================================================================      
    hivesql = []
    hivesql.append('''
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.max.dynamic.partitions=5000; 
    insert overwrite table %(dmp_spider_toredis_bd)s partition(day_id=%(dates)s,type='goods')
    select concat(site_id,'_',split(goods_id,'-')[1]) as key,concat('{\\\"goods_id\\\":\\\"',split(goods_id,'-')[1],'\\\",\\\"site_id\\\":\\\"',site_id,'\\\",\\\"site_cate_id\\\":\\\"',site_cate_id,'\\\",\\\"site_cate_name\\\":\\\"',case when site_cate_name is not null then site_cate_name else '' end,'\\\",\\\"title\\\":\\\"',title,'\\\",\\\"price\\\":\\\"',price,'\\\",\\\"std_cate_id\\\":\\\"',std_cate_id,'\\\",\\\"std_cate_name\\\":\\\"',case when std_cate_name is not null then std_cate_name else '' end,'\\\",\\\"update_date\\\":\\\"',update_date,'\\\"}') as value     
    from %(dmp_spider_goods_bd)s 
     where day_id=%(dates)s
    ''' % vars())
    #print hivesql
    HiveExe(hivesql,name,dates) 
    
#===========================================================================================
    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
