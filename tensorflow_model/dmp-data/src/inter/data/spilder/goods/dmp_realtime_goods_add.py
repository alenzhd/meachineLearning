#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称： dmp_realtime_goods_add.py
# **  功能描述：将实时爬取的商品信息，追加到dim维表
# **  特殊说明： 
# **  输入表：  
# **          
# **  调用格式：   python dmp_realtime_goods_add.py $yyyyMMdd
# **                     
# **  输出表： 
# **           
# **  创建者:   guojy
# **  创建日期: 2015/09/28
# **  修改日志:
# **  修改日期: 2015/10/29 修改人:zhangqn 修改内容: 将实时查询结果存入HIVE库的改为按天处理
# **  修改日期:  2016/01/10 修改人: guojy 
# **  修改内容:dmp_data_goods_base_all商品库关联dim_data_brand_base(品牌库表)，
# **      dmp_data_goods_base_all商品库表添加2字段：std_brand_id:标准品牌ID,std_brand_name:标准品牌名称
# **  修改日期:  2016/01/15 修改人: guojy 
# **  修改内容:1. 将原有的dmp_data_goods_base表更改成dmp_data_goods_base_all
# **           2. 新增现场使用商品库表：
# **                   dmp_data_goods_base
# **                     goods_id
# **                     std_cate_id
# **                     std_brand_id 
# **                     site(分区)
# **  
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

def get_date_of_back_someday(day_id,time_length):
    format="%Y%m%d"
    t = time.strptime(day_id, "%Y%m%d")
    result=datetime.datetime(*time.strptime(str(day_id),format)[:6])-datetime.timedelta(days=int(time_length))
    return result.strftime(format)

#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
dates = sys.argv[3]
before3days=get_date_of_back_someday(dates,3)

try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']

    dmp_data_realtime_info = "dmp_data_realtime_info"
    dmp_data_realtime_goods_bd = "dmp_data_realtime_goods_bd"
    dmp_data_goods_base_all= "dmp_data_goods_base_all"
    dim_data_brand_base= "dim_data_brand_base"
    tb_dmp_data_goods_base ="dmp_data_goods_base"
     

    #==========================================================================================
    #目标表创建
    #=============================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(dmp_data_realtime_info)s (
            spider_value             string
        ) partitioned by (day_id string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(dmp_data_realtime_info)s'
        '''% vars())
    HiveExe(hivesql,name,dates)

    hivesql = []
    hivesql.append('''
        create table if not exists %(dmp_data_realtime_goods_bd)s (
            goods_id             string,
            site_id        string,
            site_cate_id      string,
            site_cate_name      string,
            title    string,
            price    string,
            brand_code    string,
            brand_name    string,
            std_cate_id    string,
            std_cate_name    string,
            update_date    string,
            std_brand_id   string,
            std_brand_name  string
        ) partitioned by (day_id string,site string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(dmp_data_realtime_goods_bd)s'
        '''% vars())
    HiveExe(hivesql,name,dates)
    
    hivesql = []
    hivesql.append('''
        create table if not exists %(dim_data_brand_base)s (
            brand_code   string,
            brand_name   string,
            std_brand_id  string,
            std_brand_name  string,
            site  string
        ) 
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(dim_data_brand_base)s'
        '''% vars())
    HiveExe(hivesql,name,dates)
    
    hivesql.append('''
        create table if not exists %(tb_dmp_data_goods_base)s ( 
            goods_id             string,
            std_cate_id    string,
            std_brand_id   string
        ) partitioned by (site string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(tb_dmp_data_goods_base)s'
    '''% vars())
    HiveExe(hivesql,name,dates)
   
    #==========================================================================================
    #
    #=============================================================================

    hivesql = []
    hivesql.append('''alter table %(dmp_data_realtime_info)s
    drop partition(day_id=%(dates)s)''' % vars())
    HiveExe(hivesql,name,dates)

    outputbak = tmpdata_path + '/goods/bak_output_realtime/'+dates
    os.system("hadoop fs -put "+outputbak+" "+HIVE_TB_HOME+"/"+dmp_data_realtime_info)
    hivesql = []
    hivesql.append('''alter table %(dmp_data_realtime_info)s add if not EXISTS partition (day_id=%(dates)s) location '%(dates)s' ''' % vars())
    HiveExe(hivesql,name,dates)

    #==========================================================================================
    #商品信息数据清洗
    #=============================================================================
    hivesql = []
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.exec.max.dynamic.partitions=5000;
        add file %(current_path)s/split_spider_info_map.py ;
        insert into  table  %(dmp_data_realtime_goods_bd)s partition (day_id=%(dates)s,site)
        select a.goods_id,a.site_id,a.site_cate_id,a.site_cate_name,a.title,a.price,a.brand_code,a.brand_name,
              a.std_cate_id,a.std_cate_name,a.update_date,b.std_brand_id,b.std_brand_name,a.site
        from (
        select  goods_id,site_id,site_cate_id,site_cate_name,title,price,brand_code,brand_name,std_cate_id,std_cate_name,update_date,site
         from (select transform(spider_value)
              using 'python split_spider_info_map.py'
              as (goods_id,site_id,site_cate_id,site_cate_name,title,price,brand_code,brand_name,std_cate_id,std_cate_name,update_date,site)
          from %(dmp_data_realtime_info)s  where day_id=%(dates)s
          )t where goods_id!='' and site_cate_id!='' and price!='' and title!='' and site_cate_id not like '%%http%%'
        ) a
        join (select * from %(dim_data_brand_base)s ) b
        on (a.brand_code = b.brand_code)
        group by a.goods_id,a.site_id,a.site_cate_id,a.site_cate_name,a.title,a.price,a.brand_code,a.brand_name,
              a.std_cate_id,a.std_cate_name,a.update_date,b.std_brand_id,b.std_brand_name,a.site
        ''' % vars())
    HiveExe(hivesql,name,dates)

    #===========================================================================================
    #实时查询备份数据入库到商品信息表
    #1、将base_bd数据插入base
    #2、将base表内数据按id、更新时间排序，根据id去重
    #=============================================================================

    hivesql = []
    hivesql.append('''
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.max.dynamic.partitions=5000; 
    insert into table %(dmp_data_goods_base_all)s partition (site)
     select t2.goods_id,t2.site_id,t2.site_cate_id,t2.site_cate_name,t2.title,t2.price,t2.brand_code,t2.brand_name,t2.std_cate_id,t2.std_cate_name,
           t2.update_date,t2.std_brand_id,t2.std_brand_name,t2.site  
     from %(dmp_data_realtime_goods_bd)s t2 
     where t2.day_id<=%(dates)s and t2.day_id>=%(before3days)s
     group by t2.goods_id,t2.site_id,t2.site_cate_id,t2.site_cate_name,t2.title,t2.price,t2.brand_code,t2.brand_name,t2.std_cate_id,t2.std_cate_name,
             t2.update_date,t2.std_brand_id,t2.std_brand_name,t2.site 
    ''' % vars())
    HiveExe(hivesql,name,dates)

    hivesql = []
    hivesql.append('''
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.max.dynamic.partitions=5000; 
    add jar %(mix_home)s/lib/dmp-data-udf-0.0.1-SNAPSHOT.jar;
    create temporary function RowNumberUDF as 'com.ai.hive.udf.util.RowNumberUDF';
    insert overwrite table %(dmp_data_goods_base_all)s partition (site)
    select t1.goods_id,t1.site_id,t1.site_cate_id,t1.site_cate_name,t1.title,t1.price,t1.brand_code,t1.brand_name,t1.std_cate_id,t1.std_cate_name,
           t1.update_date,t1.std_brand_id,t1.std_brand_name,t1.site 
    from
    (select goods_id,site_id,site_cate_id,site_cate_name ,title,price,brand_code,brand_name,std_cate_id,std_cate_name,update_date,std_brand_id,std_brand_name,site 
      from %(dmp_data_goods_base_all)s 
      distribute by site,goods_id sort by goods_id,update_date desc)t1 where RowNumberUDF(t1.goods_id)=1
    ''' % vars())
    HiveExe(hivesql,name,dates)
    #=================================================
    #新增现场用商品库表
    #==================================================================================
    hivesql = []
    hivesql.append('''
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.max.dynamic.partitions=5000; 
    insert overwrite table %(tb_dmp_data_goods_base)s partition (site)
    select goods_id,std_cate_id,std_brand_id,site 
      from %(dmp_data_goods_base_all)s 
      where (std_cate_id!='' and std_cate_id is not null) or (std_brand_id!='' and std_brand_id is not null)
      group by goods_id,std_cate_id,std_brand_id,site 
    ''' % vars())
    HiveExe(hivesql,name,dates)
    #===========================================================================================
    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
