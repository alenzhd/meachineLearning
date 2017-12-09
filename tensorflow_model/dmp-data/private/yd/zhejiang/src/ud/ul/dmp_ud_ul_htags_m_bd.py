#!/usr/bin/env python
# -*-coding:utf-8 -*-
#*******************************************************************************************
# **  文件名称：xxx.py
# **  功能描述：xxxxxxxxx
# **  输入表：  dmp_ci_bh
# **  输出表:   dmp_table
# **  创建者:   fengbo
# **  创建日期: yyyy/mm/dd
# **  修改日志:
# **  修改日期: yyyy/mm/dd，修改人 xxx  ，修改内容：流程简化
# ** ---------------------------------------------------------------------------------------
# **
# ** ---------------------------------------------------------------------------------------
# **
# **  程序调用格式：python ......py yyyymmdd(hh)
# **
#********************************************************************************************
# **  Copyright(c) 2013 AsiaInfo Technologies (China), Inc.
# **  All Rights Reserved.
#********************************************************************************************

import sys,os
import datetime
import time
from xml.dom.minidom import parse
import xml.dom.minidom

#设置PYTHONPATH
current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../../../../../../'
python_path = []
python_path.append(mix_home+'conf')
sys.path[:0]=python_path
print sys.path

#引入自定义包
from settings import *

def parse_xml(province,provider,name):
    DOMTree = xml.dom.minidom.parse(current_path+"/../../../../../../src/ud/ul/"+province+"_"+provider+"_config.xml")
    collection = DOMTree.documentElement
    paths = collection.getElementsByTagName("paths")[0]
    type = paths.getElementsByTagName(name)[0]
    return type.childNodes[0].data
def parse_xml_getrootdata(province,provider,name):
    DOMTree = xml.dom.minidom.parse(current_path+"/../../../../../../src/ud/ul/"+province+"_"+provider+"_config.xml")
    collection = DOMTree.documentElement
    dataElement = collection.getElementsByTagName(name)[0]
    return dataElement.childNodes[0].data

#程序开始执行
name = sys.argv[0][sys.argv[0].rfind(os.sep)+1:].rstrip('.py')
dates = sys.argv[3]
province = PROVINCE
provider = PROVIDER
try:
    dicts={}
    Pama(dicts,dates)
    ARG_OPTIME = dicts['ARG_OPTIME']
    #Start(name,dates)
    res_tb = "dmp_um_m_ht"
    tag_tb = "dmp_ud_ul_id_json"

    target_tb = "dmp_ud_ul_htags_m_bd"

    hivesql = []
    hivesql.append('''
        create table if not exists %(target_tb)s
        (
            userid string,
            json_res string
        )PARTITIONED by (province string,provider string,day_id string,mflag string)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
        location '%(HIVE_TB_HOME)s/%(target_tb)s';
    ''' % vars())
    HiveExe(hivesql, name, dates)
    
    flags = parse_xml_getrootdata(province,provider,"jointype").split(",")
    flag_index = ""
    for flag in flags:
        if flag_index != "":
            flag_index += "or mflag = '"+str(flag).strip()+"'"
        else:
            flag_index += "mflag = '"+str(flag).strip()+"'"
    hivesql=[]
    hivesql.append('''
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.exec.compress.output=false;
        set mapreduce.map.output.compress=false;
        set mapreduce.output.fileoutputformat.compress=false;
        insert overwrite table %(target_tb)s partition(province='%(province)s',provider='%(provider)s',day_id=%(ARG_OPTIME)s,mflag)
        select userid,json,mflag from (
        select userid,json,mflag,row_number() over (distribute by userid,mflag sort by score desc) as row_number
            from(
                select distinct b.m_id as userid,a.json_res as json,b.uid2id_score as score,b.mflag as mflag from
                (select userid,json_res from %(tag_tb)s where day_id=%(ARG_OPTIME)s) a join
                (select mix_m_uid,m_id,uid2id_score,mflag from %(res_tb)s where day_id=%(ARG_OPTIME)s and provider='%(provider)s' and province='%(province)s' and (%(flag_index)s)) b
                on a.userid=b.mix_m_uid
            ) aa
        ) bb where row_number <=1
    ''' % vars())
    #程序结束
    HiveExe(hivesql, name, dates)
    #End(name,dates)
#异常处理
except Exception,e:
    Except(name,dates,e)
