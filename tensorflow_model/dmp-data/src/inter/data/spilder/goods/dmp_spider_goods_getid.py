#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：dmp_spider_goods_run.py
# **  功能描述：上传与商品信息表未匹配上的商品ID到爬虫input目录，监控爬虫结果目录output，
# **            商品信息爬取完成后，入dim维表，过滤dim表中的不可用数据插入到商品信息表。
# **            商品信息入库。整理为key-value格式上传到redis
# **  特殊说明：输入商品ID，第一次执行查询15天数据，
# **            代码部署的机器需要先设置共享爬虫机器的目录：/data/ftp/ai-spider/bdx-item/
# **  输入表：  
# **          
# **  调用格式：   python dmp_spider_goods_run.py $yyyyMMdd
# **                     
# **  输出表： 
# **           
# **  创建者:   ymh
# **  创建日期: 2015/08/26
# **  修改日志:
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

def get_date_of_back_someday(day_id,time_length):
    format="%Y%m%d"
    t = time.strptime(day_id, "%Y%m%d")
    result=datetime.datetime(*time.strptime(str(day_id),format)[:6])-datetime.timedelta(days=int(time_length))
    return result.strftime(format)
remote_path= '/data/ftp/ai-spider/dmp-filter/input/'
output_path = '/data/ftp/ai-spider/dmp-filter/output/'    
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
    
#===========================================================================================
#自定义变量声明---输入结果表声明
#===========================================================================================
    source_tb_js_lt = "dmp_online.dmp_uc_tags_m_bh"
    source_tb_zj_yd = "dsp.dmp_api_query_log"
    source_tb_sh_dx ="dmp_data_id_tospider_bd"
    source_tb_base = "dmp_data_goods_base_all"

#===========================================================================================
#自定义变量声明---输出结果表声明
#===========================================================================================
    dmp_spider_goodsid_bd = "dmp_data_goodsid_tospider_bd"
    dmp_spider_info_bd = "dmp_data_spider_info_bd"
    dmp_spider_goods_bd= "dmp_data_goods_base_bd"
    dmp_spider_goods= "dmp_data_goods_base_all"
    dmp_spider_toredis_bd= "dmp_srv_toredis_bd"
    tb_dmp_data_goods_base ="dmp_data_goods_base"

#创建目标表dmp_user_address_score
#===========================================================================================
    hivesql = []
    hivesql.append('''
        create table if not exists %(dmp_spider_goodsid_bd)s (
            goods_id        string
        )partitioned by (day_id string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(dmp_spider_goodsid_bd)s'
    '''% vars())
    HiveExe(hivesql,name,dates)
    
    hivesql = []
    hivesql.append('''
        create table if not exists %(dmp_spider_info_bd)s ( 
            spider_value             string
        ) partitioned by (day_id string,type string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(dmp_spider_info_bd)s'
    '''% vars())
    HiveExe(hivesql,name,dates)
    
    hivesql = []
    hivesql.append('''
        create table if not exists %(dmp_spider_goods_bd)s ( 
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
        location '%(HIVE_TB_HOME)s/%(dmp_spider_goods_bd)s'
    '''% vars())
    HiveExe(hivesql,name,dates)
    hivesql = []
    hivesql.append('''
        create table if not exists %(dmp_spider_goods)s ( 
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
        ) partitioned by (site string)
        row format delimited
        fields terminated by '\\t'
        location '%(HIVE_TB_HOME)s/%(dmp_spider_goods)s'
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
    
#===========================================================================================
#生成未匹配的商品ID到dmp_data_goodsid_tospider_bd表
#
#===========================================================================================

    hivesql = []
    hivesql.append('''
    add file  %(current_path)s/split_json_goodsid_map.py ;
    insert overwrite table %(dmp_spider_goodsid_bd)s partition(day_id=%(dates)s) select t3.goods_id from
    (select t1.goods_id from 
        (select value as goods_id from %(source_tb_js_lt)s 
            where value_type_id='2' and day_id <='%(dates)s' and day_id>='%(before3days)s' and value!=''
                and  split(value,'-')[0] in ('jd','taobao','tmall','suning','dangdang','jumei','yhd')
        union all
        select zjyd.goods_id from (select transform(keyvalue) using 'python split_json_goodsid_map.py' as goods_id  
      from %(source_tb_zj_yd)s where day_id <='%(dates)s' and day_id>='%(before3days)s') zjyd where split(zjyd.goods_id,'-')[0] in ('jd','taobao','tmall','suning','dangdang','jumei','yhd')
      union all
        select goods_id from %(dmp_spider_goods)s where update_date<='%(before30days)s'
        ) t1 group by t1.goods_id
    )t3 left outer join (select goods_id from %(dmp_spider_goods)s where update_date>='%(before30days)s' group by goods_id)  t2 on (t3.goods_id = t2.goods_id)
    where t2.goods_id is null group by t3.goods_id                 
    '''% vars())
    HiveExe(hivesql,name,dates)

#===========================================================================================
#上传未匹配的商品ID到爬虫input文件夹
#生成文件格式：20150613-150000-item.txt.final  (年月日-时分秒.txt.final)
#===========================================================================================
    inputdir = tmpdata_path + '/goods/input/'
    if not os.path.isdir(inputdir):  
        os.makedirs(inputdir)           
    #filename=str(dates[0:8])+'-'+str(dates[8:10])+'0000-item.txt.final'
    filename=str(dates[0:8])+'-'+'000000-item.txt.final'
    inputfile = tmpdata_path + '/goods/input/' + filename 
#===========================================================================================
    start_dayid=str(dates[0:8])
    days = datetime.date(int(start_dayid[0:4]),int(start_dayid[4:6]),int(start_dayid[6:8]))
    subdate = datetime.date(days.year,days.month,days.day) - datetime.timedelta(days=1)
    yestoday=subdate.strftime('%Y%m%d')
    
    hivesql="    hive -e \" "
    hivesql+=""" use %(HIVE_DATABASE)s;
    select goods_id from %(dmp_spider_goodsid_bd)s where day_id=%(dates)s
    """ % vars()
    hivesql+="\" >"+inputfile
    sys.stdout.flush()
    status = os.system(hivesql)
    print status
#==========================================================================================
#上传增量的商品ID文件
#==========================================================================================
    if len(open(inputfile).read())>0:
        os.system("cp "+inputfile+" "+remote_path)
        print "上传文件"+filename+"成功！"
    else:
        os.system("cp "+inputfile+" "+output_path)
        print "无新商品id发现！"
   #程序结束
    End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
