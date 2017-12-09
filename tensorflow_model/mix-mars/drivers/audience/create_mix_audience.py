#-*- coding:utf-8 -*-
'''
Created on 2017年5月4日

@author: Administrator
'''

import sys, os

current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../'
python_path = []
python_path.append(mix_home+'/common/')
sys.path[:0]=python_path
print 'sys.path=%s' % sys.path

import time
import subprocess
import json
from hql_condition_util import getTagsCondition, getDayIdsCondition
from mysql_update_util import updateTableValue

reload(sys)
sys.setdefaultencoding('utf-8')

ISOTIMEFORMAT='%Y-%m-%d %H:%M:%S'
SORUCE_DATABASE='dmp_online'
STAND_TAGS_TABLE = 'dmp_ud_user_stdtags_dt'
PROFILE_TAGS_TABLE = 'dmp_ud_user_profile_dt'
COMM_COND = "net_type='mobile' AND user_type='mobile'"


MIX_UID_AUDIENCE = ''' INSERT INTO TABLE dmp_srv_audience_data 
    PARTITION (audienceID) 
    SELECT mix_uid, audienceID
    FROM %(target_table)s 
    WHERE audienceID=\'%(conditions)s\'
    GROUP BY mix_uid, audienceID
    '''
STAND_TAGS =  ''' INSERT INTO TABLE dmp_srv_audience_detail 
    PARTITION (audienceID) 
    SELECT mix_uid, tags, type_id, province, '%(audience_id)s'
    FROM %(target_table)s 
    WHERE %(conditions)s
    '''
PROFILE_TAGS =''' INSERT INTO TABLE dmp_srv_audience_detail 
    PARTITION (audienceID) 
    SELECT mix_uid, tags, profile_type, province, '%(audience_id)s'
    FROM %(target_table)s 
    WHERE %(conditions)s
    '''
PARTITION_DYNAMIC='''
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.max.dynamic.partitions.pernode=50000;
    set  hive.exec.max.dynamic.partitions=50000;
    set hive.exec.max.created.files=900000;
    '''    
MIX_AUDIENCE_UV='''SELECT '%(conditions)s', COUNT(*) 
    FROM %(target_table)s 
    WHERE audienceID='%(conditions)s'
    '''

def HiveExe(hql, data_base, queue):
    "客户端运行hql"
    cmd = "hive -e \" use "+data_base+";set mapreduce.job.queuename="+queue+";set mapreduce.map.memory.mb=2048;set mapreduce.reduce.memory.mb=5120;set hive.groupby.skewindata=true;"
    for sql in hql:
        cmd = cmd + sql + ";"
    cmd =cmd + "\""
    
    maxCount = 3  #最大重试次数
    status = 1
    count = 1
    while status != 0 and count <= maxCount:
        print "StartSql:"+time.strftime( ISOTIMEFORMAT, time.localtime() )
        print '------cmd------: %s\n' % cmd
        sys.stdout.flush()
        status = os.system(cmd)
        sys.stdout.flush()
        print "EndSql:"+time.strftime( ISOTIMEFORMAT, time.localtime() )
        sys.stdout.flush()
        if status == 0 :
            return 0
        tmpLog = '第'+str(count)+'执行错误，重新执行！'
        print tmpLog
        count = count + 1
    else:
        tmpLog = '执行次数超过最大重试次数【'+str(maxCount)+'】，退出执行！'
        print tmpLog
        return 1
    
    
def HiveExe2(hql, data_base, queue):
    "客户端运行hql"
    cmd = "hive -e \" use "+data_base+";set mapreduce.job.queuename="+queue+";set mapreduce.map.memory.mb=2048;set mapreduce.reduce.memory.mb=5120;set hive.groupby.skewindata=true;"
    for sql in hql:
        cmd = cmd + sql + ";"
    cmd =cmd + "\""
    
    maxCount = 3  #最大重试次数
    status = 1
    count = 1
    while status != 0 and count <= maxCount:
        print "StartSql:"+time.strftime( ISOTIMEFORMAT, time.localtime() )
        print '------cmd------: %s\n' % cmd
        try:
            std_out = subprocess.check_output(cmd, shell=True)
            print "EndSql:"+time.strftime( ISOTIMEFORMAT, time.localtime() )
            print 'std_out=', std_out
            return 0, std_out
        except Exception, e:
            tmpLog = '第'+str(count)+'执行错误，重新执行！'
            print tmpLog
            print e
            count = count + 1
    else:
        tmpLog = '执行次数超过最大重试次数【'+str(maxCount)+'】，退出执行！'
        print tmpLog
        return 1

    
def mix_data_audience(provinces, tags, audience_id):
    #更新报告生成状态--10%
    print '开始生成品联人群...'
    updateTableValue('audience', 'state', '10', 'audience_id', audience_id)
    std_tags_cond, pro_tags_cond = getTagsCondition(tags)
    if std_tags_cond != '':
        pro_cond = getDayIdsCondition(STAND_TAGS_TABLE, provinces)
        std_cond = COMM_COND+" AND "+pro_cond+" AND "+std_tags_cond
        args_sql = {'target_table':SORUCE_DATABASE+'.'+STAND_TAGS_TABLE, 'audience_id':audience_id, 'conditions':std_cond}
        #标准人群详情
        std_tags_detail_tmp = PARTITION_DYNAMIC + STAND_TAGS % args_sql
        print '开始获取标准人群详情...'
        status = HiveExe([std_tags_detail_tmp], 'mixreport', 'default')
        if status != 0:
            print '获取标准人群详情失败!'
            return 1
        print '获取标准人群详情成功'
    else:
        print '未选择标准标签%s' % tags
    
    if pro_tags_cond != '':
        #更新报告生成状态--30%
        updateTableValue('audience', 'state', '30', 'audience_id', audience_id)
        pro_cond = getDayIdsCondition(PROFILE_TAGS_TABLE, provinces)
        cont_cond = COMM_COND+" AND "+pro_cond+" AND "+pro_tags_cond
        args_sql = {'target_table':SORUCE_DATABASE+'.'+PROFILE_TAGS_TABLE, 'audience_id':audience_id, 'conditions':cont_cond}
        #兴趣人群详情
        cont_tags_detail_tmp = PARTITION_DYNAMIC + PROFILE_TAGS % args_sql
        print '开始获取兴趣人群详情...'
        status = HiveExe([cont_tags_detail_tmp], 'mixreport', 'default')
        if status != 0:
            print '获取兴趣人群详情失败!'
            return 1
        print '获取兴趣人群详情成功'
    else:
        print '未选择兴趣标签%s' % tags
        
    args_sql = {'target_table':'mixreport.dmp_srv_audience_detail', 'conditions':audience_id}
    #品联人群ID
    mix_uid_tmp = PARTITION_DYNAMIC + MIX_UID_AUDIENCE % args_sql
    print '开始获取品联人群ID...'
    #更新报告生成状态--50%
    updateTableValue('audience', 'state', '50', 'audience_id', audience_id)
    status = HiveExe([mix_uid_tmp], 'mixreport', 'default')
    if status != 0:
        print '获取品联人群ID失败!'
        return 1
    print '获取品联人群ID成功'
    #更新报告生成状态--80%
    updateTableValue('audience', 'state', '80', 'audience_id', audience_id)
    args_sql = {'target_table':'mixreport.dmp_srv_audience_data', 'conditions':audience_id}
    audience_uv_tmp = MIX_AUDIENCE_UV % args_sql
    status, std_out = HiveExe2([audience_uv_tmp], 'mixreport', 'default')
    print 'status= %s, std_out= %s' % (str(status), std_out)
    if status == 0:
        std_out_split =  std_out.strip().split('\t')
        if len(std_out_split) == 2:
            valus_js = json.dumps({'out_uv':std_out_split[1]})
            print 'value_js='+valus_js
            updateTableValue('audience', 'value', valus_js, 'audience_id', audience_id)
    else:
        print '获取品联人群UV失败'
    print '获取品联人群UV成功'
    #更新报告生成状态--80%
    updateTableValue('audience', 'state', '100', 'audience_id', audience_id)
    completed_time = time.strftime(ISOTIMEFORMAT, time.localtime())
    updateTableValue('audience', 'update_time', completed_time, 'audience_id', audience_id)
    updateTableValue('audience', 'is_run', '0', 'audience_id', audience_id)
    print '生成品联人群成功'
    return status
    

if __name__ == '__main__':
    args = sys.argv
    if len(args) == 4:
        #provinces tags audience_id
        mix_data_audience(args[1], args[2], args[3])
    else:
        print '参数输入错误：create_mix_audience.py provinces, tags, audience_id'

    
