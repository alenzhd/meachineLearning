#-*- coding:utf-8 -*-
'''
Created on 2017年5月4日

@author: Administrator
'''

import sys
import subprocess
import logging
import time

HIVE_TB_HOME='/user/hive/warehouse/dmp_online.db'
#===========================================================================================
#因采取省份轮休，获取最近一天的日期
#===========================================================================================
def getLatestDayId(target_tb, province, provider='dxy', net_type='mobile'):
    "获取相关表和省份的最近一天分区"
    filepath = HIVE_TB_HOME + '/' + target_tb + '/provider='+provider+'/province='+province+'/net_type='+net_type
    cat = subprocess.Popen(['hadoop', 'fs', '-du',filepath],stdout=subprocess.PIPE)
    res = '0'
    yesterday = time.strftime('%Y%m%d',time.localtime(time.time()-86400))
    for line in cat.stdout:
        if 'day_id=' in line :
            tmp = line.split('=')[-1].strip()
            if int(tmp) > int(res) and int(tmp) < int(yesterday):
                res = tmp
    return res


def getDayIdsCondition(target_tb, provinces, provider='dxy', net_type='mobile'):
    print "开始获取省份的最近一天：target_tb="+target_tb+", provinces="+provinces
    logging.info("开始获取省份的最近一天：target_tb="+target_tb+", provinces="+provinces)
    ALL_PROVIENCES = 'anhui,beijing,chongqing,fujian,gansu,guangdong,guangxi,guizhou,hainan,hebei,heilongjiang,henan,hubei,hunan,jiangsu,jiangxi,jilin,liaoning,neimenggu,ningxia,qinghai,shandong,shanghai,shanxi,shanxisheng,sichuan,tianjin,xinjiang,xizang,yunnan,zhejiang'
    pro_day_id = []
    if provinces == 'ALL':
        provinces = ALL_PROVIENCES
    pro_list = provinces.strip().split(',')
    for pro in pro_list:
        day_id = getLatestDayId(target_tb, pro, provider, net_type)
        if day_id.rstrip() == '0':
            print '查找不到对应的day_id:'+'target_tb='+target_tb+', province='+pro+', provider='+provider+', net_type='+net_type
        else:
            pd = '(province=\''+pro+'\' AND day_id=\''+day_id+'\')'
            pro_day_id.append(pd)
            print pd
    if len(pro_list) >= 2:
        rs = '(' + ' OR '.join(pro_day_id) + ')'
    elif len(pro_list) == 1:
        rs = ' OR '.join(pro_day_id)
    else:
        rs = ''
    print "rs="+rs
    logging.info("rs="+rs)
    return rs


#tags_str: 1_106,2_206,3_155,4_1009,...
def getTagsCondition(tags_str):
    "接口自定义和品联标签类型mapping"
    print "开始获取标签信息", tags_str
    logging.info("开始获取标签信息%s", tags_str)
    type_id_mapping = {'1':'c', '2':'cont', '3':'b', '4':'d'}
    std_tags = []
    profile_tags = []
    type_id_tags_list = tags_str.strip().split(',') 
    for tag in type_id_tags_list :
        tag_split = tag.strip().split('_', 1)
        if len(tag_split) == 2:
            api_type_id = tag_split[0]
            if api_type_id == '2':
                if api_type_id in type_id_mapping:
                    type_id = type_id_mapping[api_type_id]
                    tmp = '(profile_type=\'' + type_id + '\' AND tags=\'' + tag_split[1] + '\')'
                    profile_tags.append(tmp)
                    print tmp
                    logging.info(tmp)
                else:
                    print '类型错误：', tag
                    logging.error('类型错误：%s', tag)
            else:
                if api_type_id in type_id_mapping:
                    type_id = type_id_mapping[api_type_id]
                    tmp = '(type_id=\'' + type_id + '\' AND tags=\'' + tag_split[1] + '\')'
                    std_tags.append(tmp)
                    print tmp
                    logging.info(tmp)
                else:
                    print '类型错误：', tag
                    logging.error('类型错误：%s', tag)
        else:
            print "标签格式不正确：", tag
            print "正确格式：typeID_tagId"
    #app, kw, site标签条件
    if len(std_tags) >= 2:
        std_tags_str =  '(' + ' OR '.join(std_tags) + ')'
    elif len(std_tags) == 1:
        std_tags_str = ' OR '.join(std_tags)
    else:
        std_tags_str = ''
    #兴趣标签条件
    if len(profile_tags) >= 2:
        profile_tags_str = '(' + ' OR '.join(profile_tags) + ')'
    elif len(profile_tags) == 1:
        profile_tags_str = ' OR '.join(profile_tags)
    else:
        profile_tags_str = ''
    print "std_tags_str="+std_tags_str, "profile_tags_str="+profile_tags_str
    logging.info("std_tags_str=%s, profile_tags_str=%s", std_tags_str, profile_tags_str)
    return std_tags_str, profile_tags_str


def getProviences():
    "获取dmp_um_user_rel_dt：用户关系累计表的所有省份"
    root_path = '/user/hive/warehouse/dmp_online.db/dmp_um_user_rel_dt/provider=dxy/'
    cat = subprocess.Popen(['hadoop', 'fs', '-du',root_path],stdout=subprocess.PIPE)
    res = []
    for line in cat.stdout:
        if 'province=' in line :
            tmp = line.split('=')[-1].strip()
            if tmp != '':
                res.append(tmp)
            else:
                print '获取省份信息失败：', line
    return ','.join(res)

def getUserRelDayIds():
    "获取dmp_um_user_rel_dt：用户关系累计表的所有省份的最近一天ID"
    pvs = getProviences()
    day_ids = getDayIdsCondition("dmp_um_user_rel_dt", pvs)
    return day_ids

if __name__ == '__main__':
    args = sys.argv
#     std_tags, profile_tags = getTagsCondition(args[1])
#     print 'std_tags='+std_tags+'\n'
    ds = getUserRelDayIds()
    print 'day_ids=', ds
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
