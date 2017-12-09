#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# ** 
# ** 品牌库更新脚本，从商品爬虫文件中获取新增品牌ID、品牌名称。
# **  经过数据处理后添加到品牌标签定义表、品牌库表。
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


SOURCE_TB_PATH =current_path+'/source_tb/'
TARGET_TB_PATH =current_path+'/target_tb/'
if not os.path.isdir(SOURCE_TB_PATH): 
    os.makedirs(SOURCE_TB_PATH) 
if not os.path.isdir(TARGET_TB_PATH): 
    os.makedirs(TARGET_TB_PATH) 

output_path = '/data/ftp/ai-spider/dmp-filter/output/' 

#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
dates=  sys.argv[3]

try:
    #传递日期参数
    dicts={}
    Pama(dicts,dates)
    Start(name,dates)
    print "test:"+dicts['ARG_OPTIME']
    ARG_OPTIME = dicts['ARG_OPTIME']
    

#===========================================================================================
#自定义表声明
#===========================================================================================
    dim_std_brand = "dmp_std_brand"
    dim_data_brand_base = "dmp_data_brand_base"
    new_site_brand = "dmp_site_brand_tmp"
    new_site_brand_base = "dmp_site_brand_base_tmp"
    tmp_match_std_brand = "dmp_brand_match_tmp"
    tmp_std_brand = "dmp_std_brand_tmp"

#=====源表创建===============================================================================    
    
    hivesql = []
    hivesql.append('''
      create table if not exists %(dim_std_brand)s
      (
        std_brand_id  int,                                   
        std_brand_name   string 
      )
      row format delimited
      fields terminated by '\\t'
      location '%(HIVE_TB_HOME)s/%(dim_std_brand)s';
    ''' % vars())
    HiveExe(hivesql,name,dates)

    hivesql = []
    hivesql.append('''
      create table if not exists %(dim_data_brand_base)s
      (
         brand_code              string,
         brand_name              string,
         std_brand_id            string,
         std_brand_name          string,
         site                    string
      )
      row format delimited
      fields terminated by '\\t'
      location '%(HIVE_TB_HOME)s/%(dim_data_brand_base)s';
    ''' % vars())
    HiveExe(hivesql,name,dates)

#=====创建表===============================================================================
    hivesql = []
    hivesql.append('''
      create table if not exists %(new_site_brand)s
      (
            pos string,
            std_brand_name string
      )
      row format delimited
      fields terminated by '\\t'
      location '%(HIVE_TB_HOME)s/%(new_site_brand)s';
    ''' % vars())
    HiveExe(hivesql,name,dates)

    hivesql = []
    hivesql.append('''
      create table if not exists %(new_site_brand_base)s
      (
        brand_code string,
        brand_name string,
        pos string,
        std_brand_name string,
        site string
      )
      row format delimited
      fields terminated by '\\t'
      location '%(HIVE_TB_HOME)s/%(new_site_brand_base)s';
    ''' % vars())
    HiveExe(hivesql,name,dates)

    hivesql = []
    hivesql.append('''
      create table if not exists %(tmp_match_std_brand)s
      (
        equal_brand string,
        new_brand string
      )
      row format delimited
      fields terminated by '\\t'
      location '%(HIVE_TB_HOME)s/%(tmp_match_std_brand)s';
    ''' % vars())
    HiveExe(hivesql,name,dates)

    hivesql = []
    hivesql.append('''
      create table if not exists %(tmp_std_brand)s
      (
        std_brand_id string,
        std_brand_name string,
        pos string
      )
      row format delimited
      fields terminated by '\\t'
      location '%(HIVE_TB_HOME)s/%(tmp_std_brand)s';
    ''' % vars())
    HiveExe(hivesql,name,dates)
#===============================================
#监控爬虫结果：
#每两分钟扫描output目录下的文件，提取新增文件数据的品牌信息
#===========================================================
    filename=str(dates[0:8])+'-000000-item.txt.final'
    filepath= output_path+'/'+filename
    pythonfile = current_path+'/split_spider_brand.py'
    new_brand_handle = current_path +'/new_brand_handle.py'
    ask = 0 
    while True:
        if  os.path.exists(filepath):
            print "执行品牌数据提取操作" 
            os.system("cat "+filepath+" | python "+pythonfile+"|sort -u|uniq  >"+SOURCE_TB_PATH+"/get_site_brands.txt")
            print "执行品牌标准化、相同品牌归并" 
            os.system("python "+new_brand_handle)
            break
        else:
            ask+=1
            print ">>>>>输出文件"+filename+"未生成，十分钟后会进行第"+str(ask)+"次尝试>>>>>" 
            time.sleep(600)  


#=====加载数据============================================
    site_std_brand =SOURCE_TB_PATH +'/site_std_brand.txt'
    site_data_brand_base = SOURCE_TB_PATH + '/site_data_brand_base.txt'
    hivesql = []
    hivesql.append('''
    load data local inpath '%(site_std_brand)s'  overwrite into table %(new_site_brand)s ;
    load data local inpath '%(site_data_brand_base)s' overwrite into table %(new_site_brand_base)s ;

    ''' % vars())
    HiveExe(hivesql,name,dates)


#=====已有品牌、新增品牌的品牌ID处理========================
    hivesql = []
    hivesql.append('''
    set mapreduce.input.fileinputformat.split.maxsize=10000000;
    set mapreduce.input.fileinputformat.split.minsize.per.node=10000000;
    set mapreduce.input.fileinputformat.split.minsize.per.rack=10000000;
    set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
    insert overwrite table %(tmp_match_std_brand)s
    select case when b.std_brand_name is not null then concat_ws('#,#',cast(b.std_brand_id as string),b.std_brand_name,a.pos) end,
        case when b.std_brand_name is null then concat_ws('#,#',a.std_brand_name,a.pos) end
    from %(new_site_brand)s a
    left outer join %(dim_std_brand)s b
    on (a.std_brand_name = b.std_brand_name)
    group by case when b.std_brand_name is not null then concat_ws('#,#',cast(b.std_brand_id as string),b.std_brand_name,a.pos) end,
        case when b.std_brand_name is null then concat_ws('#,#',a.std_brand_name,a.pos) end
    ''' % vars())
    HiveExe(hivesql,name,dates)


#=====求std_brand_id的最大值
    filename = SOURCE_TB_PATH+'/maxbrandid.txt'
    hivesql="    hive -e \" "
    hivesql+=""" use %(HIVE_DATABASE)s;
        select max(std_brand_id) from %(dim_std_brand)s;
    """ % vars()
    hivesql+="\" >"+filename
    sys.stdout.flush()
    status = os.system(hivesql)
    print hivesql
    print status

    MAX_BRAND_ID = open(SOURCE_TB_PATH +"//maxbrandid.txt").read().strip('\n')
    print "MAX_BRAND_ID:"+MAX_BRAND_ID

    hivesql = []
    hivesql.append('''
    set mapreduce.input.fileinputformat.split.maxsize=10000000;
    set mapreduce.input.fileinputformat.split.minsize.per.node=10000000;
    set mapreduce.input.fileinputformat.split.minsize.per.rack=10000000;
    set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
    add file %(current_path)s/sort_brand_id.py;
    insert overwrite table %(tmp_std_brand)s
    select std_brand_id,std_brand_name,pos
    from (
    select split(equal_brand,'#,#')[0] as std_brand_id,split(equal_brand,'#,#')[1] as std_brand_name
           ,split(equal_brand,'#,#')[2] as pos
    from  %(tmp_match_std_brand)s where equal_brand is not null and equal_brand!=''
    union all
    select  transform('%(MAX_BRAND_ID)s',split(new_brand,'#,#')[0],split(new_brand,'#,#')[1])
        using 'python sort_brand_id.py'
        as (std_brand_id,std_brand_name,pos)
    from  %(tmp_match_std_brand)s  where new_brand is not null and new_brand!=''
    ) a
    ''' % vars())
    HiveExe(hivesql,name,dates)

#=====标签品牌定义表、品牌库表数据文件生成================================= 

    std_brand_file = TARGET_TB_PATH+'/dim_std_brand_'+str(dates[0:8])+'.txt'
    hivesql="    hive -e \" "
    hivesql+=""" use %(HIVE_DATABASE)s;
    select a.std_brand_id,a.std_brand_name
    from (select std_brand_id,std_brand_name from  %(tmp_std_brand)s group by std_brand_id,std_brand_name) a
    left outer join %(dim_std_brand)s b
    on (a.std_brand_name = b.std_brand_name)
    where b.std_brand_name is null
    group by  a.std_brand_id,a.std_brand_name
    """ % vars()
    hivesql+="\" >"+std_brand_file
    sys.stdout.flush()
    status = os.system(hivesql)
    print hivesql
    print status


    std_brand_base_file = TARGET_TB_PATH+'/dim_data_brand_base_'+str(dates[0:8])+'.txt'
    hivesql="    hive -e \" "
    hivesql+=""" use %(HIVE_DATABASE)s;
    select k.code ,k.name,k.std_id,k.std_name,k.site
    from (
        select  b.brand_code code,b.brand_name name,a.std_brand_id std_id,a.std_brand_name std_name,b.site site
        from(select * from  %(tmp_std_brand)s ) a
        join (select * from  %(new_site_brand_base)s ) b
        on (a.std_brand_name = b.std_brand_name)
        ) k
    left outer join %(dim_data_brand_base)s h
    on (k.code = h.brand_code and k.name = h.brand_name and k.std_id = h.std_brand_id 
         and k.std_name = h.std_brand_name  and k.site = h.site)
    where h.brand_code is null and h.brand_name is null and h.std_brand_id is null
        and h.std_brand_name is null and h.site is null
    """ % vars()
    hivesql+="\" >"+std_brand_base_file
    sys.stdout.flush()
    status = os.system(hivesql)
    print hivesql
    print status

#=====hive中的标签品牌定义表、品牌库表增量更新=================
    hivesql = []
    hivesql.append('''
    load data local inpath '%(std_brand_file)s' into table %(dim_std_brand)s ;
    load data local inpath '%(std_brand_base_file)s' into table %(dim_data_brand_base)s ;

    ''' % vars())
    HiveExe(hivesql,name,dates)

    #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)


