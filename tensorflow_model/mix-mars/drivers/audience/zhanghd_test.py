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
from hql_condition_util import getUserRelDayIds
from mysql_update_util import updateTableValue

reload(sys)
sys.setdefaultencoding('utf-8')
#配置日志输出
# logging.config.fileConfig("./config/logger.conf")
# logger = logging.getLogger("example02")

ISOTIMEFORMAT='%Y-%m-%d %X'
SORUCE_DATABASE='dmp_online'
DMP_UM_USER_REL_DT = 'dmp_um_user_rel_dt'
COMM_COND = "net_type='mobile' AND user_type='mobile' AND provider='dxy'"


DMP_SRV_AUDIENCE_IN_ADD_PARTITON = ''' 
    ALTER TABLE dmp_srv_audience_in ADD IF NOT EXISTS
    PARTITION (audienceid = '%s') LOCATION '%s'
    '''
USER_IN_UV =  '''
SELECT COUNT(m.mix_uid) 
    FROM (SELECT mix_uid
    FROM dmp_srv_audience_in
    WHERE audienceid = '%s'
    GROUP BY mix_uid
    ) m
    '''
# DMP_SRV_AUDIENCE_IN_TMP =''' INSERT OVERWRITE TABLE dmp_srv_audience_in_tmp
#     SELECT mix_uid, audienceid
#     FROM dmp_srv_audience_in
#     WHERE audienceid = '%s'
#     '''
DMP_SRV_AUDIENCE_IN_TMP =''' CREATE TABLE %(tmp_table)s
    AS SELECT mix_uid, audienceid
    FROM dmp_srv_audience_in
    WHERE audienceid = '%(audience_id)s'
    '''
DROP_TMP_TABLE = '''
    DROP TABLE IF EXISTS %(tmp_table)s
    '''
DMP_UM_USER_REL_DT_TMP = '''CREATE TABLE dmp_um_user_rel_dt_tmp_%(audience_id)s
    AS SELECT mix_uid, flag_id, province, day_id, flag
    FROM dmp_online.dmp_um_user_rel_dt
    WHERE %(conditions)s
    '''
DMP_UM_USER_REL_DT_TMP_MD5 = '''CREATE TABLE dmp_um_user_rel_dt_tmp_%(audience_id)s
    AS SELECT mix_uid, md5(flag_id) as flag_id, province, day_id, flag
    FROM dmp_online.dmp_um_user_rel_dt
    WHERE %(conditions)s
    '''
DMP_UM_USER_REL_DT_TMP_SHA1 = '''CREATE TABLE dmp_um_user_rel_dt_tmp_%(audience_id)s
    AS SELECT mix_uid, sha1(flag_id) as flag_id, province, day_id, flag
    FROM dmp_online.dmp_um_user_rel_dt
    WHERE %(conditions)s
    '''
DMP_SRV_AUDIENCE_DETAIL_ADD_PARTITION = '''INSERT INTO TABLE dmp_srv_audience_detail 
    PARTITION (audienceID) 
    SELECT b.mix_uid, b.flag_id, b.flag, b.province, a.audienceID
    FROM %(tmp_table)s a LEFT OUTER JOIN dmp_um_user_rel_dt_tmp_%(audience_id)s b ON lower(a.mix_uid)=lower(b.flag_id)
    WHERE b.mix_uid IS NOT NULL
    '''
DMP_SRV_AUDIENCE_DATA_ADD_PARTITION = '''INSERT INTO TABLE dmp_srv_audience_data 
    PARTITION (audienceID) 
    SELECT a.mix_uid, a.audienceID
    FROM dmp_srv_audience_detail a
    WHERE a.audienceid='%(audience_id)s'
    GROUP BY a.mix_uid, a.audienceID
    '''
MIX_UID_OUT_UV = '''
SELECT COUNT(m.mix_uid) 
    FROM (SELECT mix_uid
    FROM dmp_srv_audience_data
    WHERE audienceid = '%s'
    GROUP BY mix_uid) m
    '''
USER_OUT_UV = '''
    select count(distinct lower(tags)) 
    from dmp_srv_audience_detail 
    where audienceid = '%s'
    '''
PARTITION_DYNAMIC='''
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.max.dynamic.partitions.pernode=50000;
    set  hive.exec.max.dynamic.partitions=50000;
    set hive.exec.max.created.files=900000;
    add jar /data1/user/zhanghd/mix-dmp-udf-1.0.0.jar;
    create temporary function MD5func as 'com.ai.hive.udf.util.Md5UDF';
    create temporary function SladuidEncode as 'com.ai.hive.udf.util.Sha1UDF';
    '''     

def HiveExe(hql, data_base, queue):
    "客户端运行hql"
    cmd = "hive -e \" use "+data_base+";set mapreduce.job.queuename="+queue+";set mapreduce.map.memory.mb=2048;set mapreduce.reduce.memory.mb=5120;set hive.groupby.skewindata=true;"
    for sql in hql:
        cmd = cmd + sql + ";"
        print time.strftime( ISOTIMEFORMAT, time.localtime() )
    cmd =cmd + "\""
    
    maxCount = 3  #最大重试次数
    status = 1
    count = 1
    while status != 0 and count <= maxCount:
        print "StartSql:"+time.strftime( ISOTIMEFORMAT, time.localtime() )
        print "------cmd------%s\n" % cmd
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
    
    
def ShellCmdExe(cmd):
    "执行指定的shell命令"
    maxCount = 3  #最大重试次数
    status = 1
    count = 1
    std_out = ""
    while status != 0 and count <= maxCount:
        print "StartHdfs:%s" % time.strftime( ISOTIMEFORMAT, time.localtime() )
        print "------cmd------%s\n" % cmd
        try:
            std_out = subprocess.check_output(cmd, shell=True)
            print "EndHdfs:%s" % time.strftime( ISOTIMEFORMAT, time.localtime() )
            print 'std_out='+std_out
            return 0, std_out
        except subprocess.CalledProcessError, e:
            tmpLog = '第'+str(count)+'执行错误，重新执行！'
            print tmpLog
            count = count + 1
            print e
    else:
        tmpLog = '执行次数超过最大重试次数【'+str(maxCount)+'】，退出执行！'
        print tmpLog
        return 1, std_out
    
def dropTmpTables():
    tmp_tables = [audience_id, 'dmp_um_user_rel_dt_tmp_'+audience_id]
    for tt in tmp_tables:
        hs = DROP_TMP_TABLE % {'tmp_table': tt}
        HiveExe2([hs], 'mixreport', 'default')
        print '%s临时表已删除' % tt
    return 0
    
    
def uploadUserFilesToHdfs(file_name, audience_id, user_name):
    #定义文件路径
#     hive_partition_path_prefix = "/user/hive/warehouse/dmptest_user_dir/yuanjk/dmp_srv_audience_in/"
    hive_partition_path_prefix = "/user/hive/warehouse/dmptest_user_dir/mixreport/dmp_srv_audience_in/"
    hive_partition_path = hive_partition_path_prefix + audience_id
#     user_file_path_prefix = "/data1/user/yuanjk/mix-report-api/user_files/"
    user_file_path_prefix = "/data1/user/mixreport/sftp/user/"
    user_name_prefix = user_name.strip().split('@')[0]
    user_file_path = user_file_path_prefix + user_name_prefix + '/DATA/IN/' +file_name
    #定义hdfs命令
    test_exists = "hdfs dfs -test -e %s" % hive_partition_path
    remove_dir = "hdfs dfs -rm -r %s" % hive_partition_path
    make_dir = "hdfs dfs -mkdir  %s" % hive_partition_path
    upload_file = "hdfs dfs -put %s %s" % (user_file_path, hive_partition_path)
    #创建hdfs分区路径，并上传用户ID文件到该路径
    status, std_out = ShellCmdExe(test_exists)
    if status == 0:
        print "该路径已存在："+hive_partition_path
        ShellCmdExe(remove_dir)
        ShellCmdExe(make_dir)
    else:
        print "该路径不存在："+hive_partition_path
        ShellCmdExe(make_dir)
    print "开始上传用户ID文件到hive分区："+upload_file
    status, std_out = ShellCmdExe(upload_file)
    if status == 0:
        print "上传用户ID文件到hive分区已完成"
    else:
        print "上传用户ID文件到hive分区失败："+std_out
        sys.exit(1)
    #添加hive分区
    print '开始添加用户ID分区...'
    add_partition = DMP_SRV_AUDIENCE_IN_ADD_PARTITON  % (audience_id, hive_partition_path)
    status = HiveExe([add_partition], 'mixreport', 'default')
    if status == 0:
        print '添加用户ID分区完成'
        return 0
    else:
        print '添加用户ID分区失败'
        sys.exit(1)    
    
def HiveExe2(hql, data_base, queue):
    "客户端运行hql, 并输出结果"
    cmd = "hive -e \" use "+data_base+";set mapreduce.job.queuename="+queue+";set mapreduce.map.memory.mb=2048;set mapreduce.reduce.memory.mb=5120;set hive.groupby.skewindata=true;"
    for sql in hql:
        cmd = cmd + sql + ";"
        print time.strftime( ISOTIMEFORMAT, time.localtime() )
    cmd =cmd + "\""
    
    maxCount = 3  #最大重试次数
    status = 1
    count = 1
    std_out = ''
    while status != 0 and count <= maxCount:
        print "StartSql:"+time.strftime( ISOTIMEFORMAT, time.localtime() )
        print '------cmd------%s\n' % cmd
        try:
            std_out = subprocess.check_output(cmd, shell=True)
            print "EndSql:"+time.strftime( ISOTIMEFORMAT, time.localtime() )
            print 'std_out='+std_out
            return 0, std_out
        except Exception, e:
            tmpLog = '第'+str(count)+'执行错误，重新执行！'
            print tmpLog
            print e
            count = count + 1
    else:
        tmpLog = '执行次数超过最大重试次数【'+str(maxCount)+'】，退出执行！'
        print tmpLog
        print 'std_out='+std_out
        return 1, std_out

def getUserAudienceValue(audience_id):
    # "统计自有人群信息" 少一种mix_uid
    flag_mapping = {'md5imei':'\'md532_imei\'','imei':'\'std_imei\'', 'idfa':'\'idfa\'', 'mac':'\'mac\'',
                  'md5idfa':'\'md532_idfa\'','md5mac':'\'md532_mac\'', 'sha1imei':'\'shal_imei\'',
                  'sha1idfa':'\'sha1_idfa\'','sha1mac':'\'sha1_mac\'','mix_uid':'\'mix_uid\'' }
    
    print '开始统计自有人群输入UV...'
    user_in_uv_tmp = USER_IN_UV % audience_id
    status, std_out = HiveExe2([user_in_uv_tmp], 'mixreport', 'default')
    if status == 0:
        print '自有人群输入UV：in_uv='+str(std_out)
        val = {'in_uv':std_out.strip()}
    else:
        print '自有人群输入UV统计失败：'+std_out
        sys.exit(1)
    #更新报告生成状态--40%
    updateTableValue('audience', 'state', '40', 'audience_id', audience_id)
    
    print '开始生成用户ID临时表...'
    user_id_tab_tmp = DMP_SRV_AUDIENCE_IN_TMP % {'tmp_table':audience_id, 'audience_id':audience_id}
    status, std_out = HiveExe2([user_id_tab_tmp], 'mixreport', 'default')
    if status == 0:
        print '生成用户ID临时表完成'
    else:
        print '生成用户ID临时表失败：'
        dropTmpTables()
        sys.exit(1)
    #更新报告生成状态--50%
    updateTableValue('audience', 'state', '50', 'audience_id', audience_id)
    
    print '开始生成用户关系临时表...'
    audience_id_split = audience_id.strip().split('_')
    if len(audience_id_split) == 4:
        flag_tmp = audience_id_split[2]
        flag = flag_mapping[flag_tmp.lower()]
    else:
        print '无法获取用户ID类型'
        dropTmpTables()
        sys.exit(1)
    #判断是那种加密方式
    if(flag.__contains__("md5")):
        # flag=
        flag= flag.split('_')[1]
        DMP_UM_USER_REL_DT_TMP = DMP_UM_USER_REL_DT_TMP_MD5
    elif(flag.__contains__("sha1")):
        flag= flag.split('_')[1]
        DMP_UM_USER_REL_DT_TMP = DMP_UM_USER_REL_DT_TMP_SHA1
    pvs = getUserRelDayIds()
    conditions = COMM_COND + "  AND flag=" + flag + " AND " + pvs
    user_rel_tab_tmp = DMP_UM_USER_REL_DT_TMP % {'audience_id':audience_id, 'conditions':conditions}
    status, std_out = HiveExe2([user_rel_tab_tmp], 'mixreport', 'default')
    if status == 0:
        print '生成用户关系临时表完成'
    else:
        print '生成用户关系临时表失败：'
        dropTmpTables()
        sys.exit(1)
    #更新报告生成状态--60%
    updateTableValue('audience', 'state', '60', 'audience_id', audience_id)
    
    print '开始自有人群详情匹配...'
    data_add_partition  = DMP_SRV_AUDIENCE_DETAIL_ADD_PARTITION % {'tmp_table':audience_id, 'audience_id':audience_id}
    status, std_out = HiveExe2([PARTITION_DYNAMIC + data_add_partition], 'mixreport', 'default')
    if status == 0:
        print '自有人群详情匹配成功'
    else:
        print '自有人群详情匹配失败：'+std_out
        dropTmpTables()
        sys.exit(1)
        
    print '开始删除临时表 %s...' % audience_id
    status = dropTmpTables()
    if status == 0:
        print '删除临时表 %s成功' % audience_id
    else:
        print '删除临时表 %s失败' % audience_id
        sys.exit(1)
    #更新报告生成状态--70%
    updateTableValue('audience', 'state', '70', 'audience_id', audience_id)
    
    print '开始自有人群匹配ID...'
    dmp_srv_audience_data_add_partition = DMP_SRV_AUDIENCE_DATA_ADD_PARTITION % {'audience_id':audience_id}
    status, std_out = HiveExe2([PARTITION_DYNAMIC + dmp_srv_audience_data_add_partition], 'mixreport', 'default')
    if status == 0:
        print '自有人群匹配ID成功'
    else:
        print '自有人群匹配ID失败：'+std_out
        dropTmpTables()
        sys.exit(1)
    
    print '开始统计自有人群匹配UV...'
    user_out_uv_tmp = USER_OUT_UV % audience_id
    status, std_out = HiveExe2([user_out_uv_tmp], 'mixreport', 'default')
    if status == 0:
        print '统计自有人群匹配UV：out_uv='+str(std_out)
        val['out_uv'] = std_out.strip()
    else:
        print '统计自有人群匹配UV失败：'+std_out
        sys.exit(1)
        
    print '开始统计自有人群匹配MIXUID UV...'
    mix_uid_out_uv_tmp = MIX_UID_OUT_UV % audience_id
    status, std_out = HiveExe2([mix_uid_out_uv_tmp], 'mixreport', 'default')
    if status == 0:
        print '统计自有人群匹配MIXUID UV：mixuid_out_uv='+str(std_out)
        val['mixuid_out_uv'] = std_out.strip()
    else:
        print '统计自有人群匹配MIXUID UV失败：'+std_out
        sys.exit(1)
        
    print "val=", json.dumps(val)
    return json.dumps(val)
        

if __name__ == '__main__':
    args = sys.argv
    if len(args) == 4:
        audience_id = args[1]
        file_name = args[2]
        user_name = args[3]
        #更新报告生成状态--10%
        updateTableValue('audience', 'state', '10', 'audience_id', audience_id)
        uploadUserFilesToHdfs(file_name, audience_id, user_name)
        #更新报告生成状态--30%
        updateTableValue('audience', 'state', '30', 'audience_id', audience_id)
        try:
            valus_js = getUserAudienceValue(audience_id)
        except Exception, e:
            dropTmpTables()
            print '获取自有人群uv失败'
        #更新报告生成状态--80%
        updateTableValue('audience', 'state', '80', 'audience_id', audience_id)
        updateTableValue('audience', 'value', valus_js, 'audience_id', audience_id)
        #更新报告生成状态--100%
        updateTableValue('audience', 'state', '100', 'audience_id', audience_id)
        completed_time = time.strftime(ISOTIMEFORMAT, time.localtime())
        updateTableValue('audience', 'update_time', completed_time, 'audience_id', audience_id)
        updateTableValue('audience', 'is_run', '0', 'audience_id', audience_id)
    else:
        print '参数输入错误：create_user_audience.py file_name, audience_id'

    
