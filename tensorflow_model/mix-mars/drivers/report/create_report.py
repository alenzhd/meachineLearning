#coding=utf-8
import sys, os, time, json


current_path=os.path.dirname(os.path.abspath(__file__))
mix_home=current_path+'/../'
python_path = []
python_path.append(mix_home+'/common/')
sys.path[:0]=python_path
print sys.path

from hql_condition_util import *
from mysql_update_util import *

province = sys.argv[1]
audienceIDs = sys.argv[2]
report_id = sys.argv[3]

audienceID = ''
for t in audienceIDs.strip().split(','):
    if audienceID == '':
        audienceID = "'" + t + "'"
    else:
        audienceID = audienceID + ",'" + t + "'"

ISOTIMEFORMAT='%Y-%m-%d %H:%M:%S'

def HiveExe(hivesql):
    cmd = 'beeline -u jdbc:hive2://10.1.2.161:10000 -n dmptest  --outputformat=tsv2 -e \"use dmp_online;" -e "set mapreduce.map.memory.mb=2048;" -e "set mapreduce.reduce.memory.mb=4096;" -e "set mapreduce.map.java.opts=-Xmx1800m;" -e "set mapreduce.reduce.java.opts=-Xmx3500m;" -e "set hive.exec.dynamic.partition=true;" -e "set hive.exec.dynamic.partition.mode=nonstrict;" -e "add jar /tmp/mix-dmp-udf-1.0.0.jar;" -e "create temporary function SladuidEncode as \'com.ai.hive.udf.sladuid.SladuidEncodeUDF\';" '
    for sql in hivesql:
        cmd = cmd + " -e \"" + sql + ";"
    cmd =cmd + "\""

    maxCount = 3  #最大重试次数
    status = 1
    count = 1
    while status != 0 and count <= maxCount:
        print "StartSql:"+time.strftime( ISOTIMEFORMAT, time.localtime() )
        print '---cmd---: %s\n' % cmd
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

# ====================
# 参数列表
# ====================
data={}
KPI_str = {}
sum_uv = '0'      # 总uv
audience_uv = '0' # 人群总uv
tgi_bili = 0.01   # 计算tgi时，标签的uv与人群uv的比值应大于 XX
tgi_cont_bili = 0.001 #cont_tgi
tgi_pubnum_bili = 0.0005 # 公众号计算tgi时，标签的uv与人群uv的比值应大于 XX
tgi_kw_bili = 0.0005 # 关键词计算tgi时，标签的uv与人群uv的比值应大于 XX
limit_num = 100   # limit 取前 XX
uv_min_num = 2    # 计算tgi时，过滤掉uv量小于等于XX的标签


def general(name, uv, tgi):  # 生成cont_json对象
    dict_tmp = {}
    dict_res = {}
    dict_tmp['name'] = name
    dict_tmp['uv'] = uv
    dict_tmp['tgi'] = tgi
    dict_tmp['subs'] = {}
    dict_res[name] = dict_tmp
    return dict_res

def runHiveExe(hql, param):
    global sum_uv
    global audience_uv
    cmd = 'beeline -u jdbc:hive2://10.1.2.161:10000 -n dmptest  --outputformat=tsv2 -e \"use dmp_online;" -e "set mapreduce.map.memory.mb=2048;" -e "set mapreduce.reduce.memory.mb=4096;" -e "set mapreduce.map.java.opts=-Xmx1800m;" -e "set mapreduce.reduce.java.opts=-Xmx3500m;" -e "set hive.exec.dynamic.partition=true;" -e "set hive.exec.dynamic.partition.mode=nonstrict;" -e "add jar /tmp/mix-dmp-udf-1.0.0.jar;" -e "create temporary function SladuidEncode as \'com.ai.hive.udf.sladuid.SladuidEncodeUDF\';" '
    for sql in hql:
        cmd = cmd + " -e \"" + sql
    cmd =cmd + "\""
    print cmd

    maxCount = 3  #最大重试次数
    status = 1
    count = 1
    while status != 0 and count <= maxCount:
        print "StartSql:"+time.strftime( ISOTIMEFORMAT, time.localtime() )
        print '---cmd---: %s\n' % cmd
        sys.stdout.flush()
        #status = os.system(cmd)
        status = 0
        sys.stdout.flush()
        print "EndSql:"+time.strftime( ISOTIMEFORMAT, time.localtime() )
        sys.stdout.flush()
        if status == 0 :
            output = os.popen(cmd)
            output_cmd = output.readlines()
            lines = output_cmd
            isFirstLine = True  #判断是否是第一行，是则跳过
            if param == 'UV': # 加载到变量
                for line in lines:
                    if isFirstLine == True:
                        isFirstLine = False
                        continue
                    line = line.replace('\'', '').strip()
                    # data["UV"] = line.strip()
                    audience_uv = line.strip()
                    updateTableValue('report', 'UV', audience_uv, 'report_id', report_id)
            elif param == 'sum_UV':  # 加载到变量
                for line in lines:
                    if isFirstLine == True:
                        isFirstLine = False
                        continue
                    line = line.replace('\'', '').strip()

                    sum_uv = line.strip()
                    return 0
            elif param == 'cont_UV_TGI':  # 4个参数
                data_cont = {}
                KPI_str_tmp = {}
                data1={}
                point = data_cont
                for line in lines:
                    if isFirstLine == True:
                        isFirstLine = False
                        continue
                    line = line.replace('\'', '').strip()

                    cont_name = line.split('\t')[0]
                    cont_path = line.split('\t')[1]
                    uv = float(line.split('\t')[2]) / float(audience_uv)
                    tgi = line.split('\t')[3]
                    tags = cont_path.split('/')
                    point = data_cont   # 类似指针，记录字典的当前位置
                    for i in range(len(tags)):
                        if tags[i] not in point:    # 若果如果当前位置没有有该key，则创建新的对象，并指向该key的subs
                            point.update(general(tags[i],uv,tgi))
                            point = point.get(tags[i]).get('subs')
                        else:                       # 如果当前位置有该key，则指向该key的subs
                            point = point.get(tags[i]).get('subs')
                KPI_str_tmp[param] = data_cont
                data1["KPI"] = KPI_str_tmp
                jsonStr1 = json.dumps(data1, sort_keys=True,indent =4,separators=(',', ':'),encoding="utf-8",ensure_ascii=False )
                print jsonStr1
                cont_file = open(current_path+"/../../data/cont_UV_TGI_"+report_id+".txt",'w')
                cont_file.writelines(jsonStr1)
                cont_file.close()

            elif param == 'cont_TGI':
                Param_str = []
                for line in lines:
                    if isFirstLine == True:
                        isFirstLine = False
                        continue
                    line = line.replace('\'', '').strip()
                    
                    tmp = {}
                    if line.count('\t') == 3:
                        tmp['name'] = line.split('\t')[0]
                        tmp['value'] = float(line.split('\t')[1])
                        tmp['cont_path'] = line.split('\t')[2]
                        tmp['uv'] = float(line.split('\t')[3]) / float(audience_uv)
                    else:
                        print u'格式错误：' + line
                    Param_str.append(tmp)
                KPI_str[param] = Param_str

            elif param == 'app_TGI' or param == 'site_TGI' or param == 'pubnum_TGI' or param == 'age_UV' or param == 'gender_UV' or param == 'dev_brand_UV' or param == 'dev_os_UV' or param == 'dev_type_UV' or param == 'province_UV': # 3个参数
                Param_str = []
                for line in lines:
                    if isFirstLine == True:
                        isFirstLine = False
                        continue
                    line = line.replace('\'', '').strip()
                    
                    tmp = {}
                    if line.count('\t') == 2:
                        tmp['name'] = line.split('\t')[0]
                        if param == 'province_UV' or param == 'age_UV' or param == 'gender_UV' or param == 'dev_brand_UV' or param == 'dev_os_UV' or param == 'dev_type_UV': # 性别年龄单独处理
                            tmp['value'] = float(line.split('\t')[1]) / float(line.split('\t')[2])
                        else:
                            tmp['value'] = line.split('\t')[1]
                            tmp['uv'] = float(line.split('\t')[2]) / float(audience_uv)
                    else:
                        print u'格式错误：' + line
                    Param_str.append(tmp)
                KPI_str[param] = Param_str
            else:  # 2个参数
                Param_str = []
                for line in lines:
                    if isFirstLine == True:
                        isFirstLine = False
                        continue
                    line = line.replace('\'', '').strip()
                    
                    tmp = {}
                    if line.count('\t') == 1:
                        tmp['name'] = line.split('\t')[0]
                        if param == 'province_CR' or param == 'kw_TGI':
                            tmp['value'] = line.split('\t')[1]
                        else:
                            tmp['value'] = float(line.split('\t')[1]) / float(audience_uv)
                    else:
                        print u'格式错误：' + line
                    Param_str.append(tmp)
                KPI_str[param] = Param_str
            data["KPI"] = KPI_str
            jsonStr = json.dumps(data, sort_keys=True,indent =4,separators=(',', ':'),encoding="utf-8",ensure_ascii=False )
            print jsonStr
            updateTableValue('report', 'value', jsonStr, 'report_id', report_id)
            return 0
        tmpLog = '第'+str(count)+'执行错误，重新执行！'
        print tmpLog
        count = count + 1
    else:
        tmpLog = '执行次数超过最大重试次数【'+str(maxCount)+'】，退出执行！'
        print tmpLog
        return 1

# =====================================
# 删除临时表
# =====================================

def Delete_Tmp_Table(report_id):
    hivesql = []
    hivesql.append('''
    drop table if exists mixreport.tmp_table_action_%(report_id)s
    '''  % vars() )
    HiveExe(hivesql)

    hivesql = []
    hivesql.append('''
    drop table if exists mixreport.tmp_table_dev_%(report_id)s
    '''  % vars() )
    HiveExe(hivesql)

    hivesql = []
    hivesql.append('''
    drop table if exists mixreport.tmp_table_profile_%(report_id)s
    '''  % vars() )
    HiveExe(hivesql)

    hivesql = []
    hivesql.append('''
    drop table if exists mixreport.tmp_table_pubnum_%(report_id)s
    '''  % vars() )
    HiveExe(hivesql)

    hivesql = []
    hivesql.append('''
    drop table if exists mixreport.tmp_table_cont_%(report_id)s
    '''  % vars() )
    HiveExe(hivesql)

#UDF_JAR包路径
#MIX_JAR = '/home/dmptest/lib/mix-dmp-udf-1.0.0.jar'

if __name__ == '__main__':
    condition = getDayIdsCondition('dmp_ud_user_profile_dt', province)

    Delete_Tmp_Table(report_id)


    updateTableValue('report', 'state', '5', 'report_id', report_id)
# ==========================
# 总uv
# ==========================
    print '总UV=========================================='
    hivesql = []
    hivesql.append('''
        select count(1)
        from(
            select mix_uid
            from mixreport.dmp_srv_audience_data
            where audienceID in (%(audienceID)s)
            group by mix_uid
        )t

    ''' % vars() )

    runHiveExe(hivesql,'UV')
    updateTableValue('report', 'state', '10', 'report_id', report_id)

# =========================
# 分省热图: province
# =========================
    print '地域热图=========================================='
    hivesql = []
    hivesql.append('''
        select a.province, a.uv, b.uuv
        from(
            select province, count(1) as uv
            from(
                select mix_uid, province
                from mixreport.dmp_srv_audience_detail
                where audienceID in (%(audienceID)s)
                group by mix_uid, province
            )t2
            group by province
        )a
        join(
            select count(1) as uuv
            from(
                select mix_uid, province
                from mixreport.dmp_srv_audience_detail
                where audienceID in (%(audienceID)s)
                group by mix_uid, province
            )t3
        )b
        group by a.province, a.uv, b.uuv
    ''' % vars() )

    runHiveExe(hivesql,'province_UV')

    print '地域覆盖率coverage rate=========================================='
    hivesql = []
    hivesql.append('''
        select a.province, a.uv / b.uv as midu
        from(
            select province, count(1) as uv
            from(
                select mix_uid, province
                from mixreport.dmp_srv_audience_detail
                where audienceID in (%(audienceID)s)
                group by mix_uid, province
            )t
            group by province
        )a
        join(
            select tags, uv
            from mixreport.dim_tag_count
            where type = 'province'
        )b
        on(a.province = b.tags)

    ''' % vars() )

    runHiveExe(hivesql,'province_CR')
    updateTableValue('report', 'state', '15', 'report_id', report_id)

# ============================
# 性别年龄
# ============================

    print '年龄性别=========================================='
    hivesql = []
    hivesql.append('''
        create table mixreport.tmp_table_profile_%(report_id)s as
        select profile_type, tags, count(1) as uv
        from(
            select t2.profile_type, t2.tags, t2.mix_uid
            from(
                select mix_uid
                from mixreport.dmp_srv_audience_data
                where audienceID in (%(audienceID)s)
                group by mix_uid
            )t1
            join(
               select mix_uid, tags, profile_type
               from dmp_online.dmp_ud_user_profile_dt
               where profile_type in ('dg_age','dg_gender')
               and user_type = 'mobile' and tags like '8%%' and %(condition)s
               group by mix_uid, tags, profile_type
            )t2
            on (t1.mix_uid = t2.mix_uid)
            group by t2.profile_type, t2.tags, t2.mix_uid
        )t
        group by profile_type, tags
        ''' %vars() )

    HiveExe(hivesql)

    print '年龄=========================================='
    hivesql = []
    hivesql.append('''
        select b.cont_name, a.uv, c.sum_uv
        from(
            select tags, uv
            from mixreport.tmp_table_profile_%(report_id)s
            where profile_type = 'dg_age'
        )a
        join(
            select cont_id, cont_name
            from dmp_console.dim_cont
            where state = '1'
        )b
        on (a.tags = b.cont_id)
        join (
            select sum(uv) as sum_uv
            from mixreport.tmp_table_profile_%(report_id)s
            where profile_type = 'dg_age'
        )c
    ''' %vars() )
    runHiveExe(hivesql,'age_UV')
    updateTableValue('report', 'state', '20', 'report_id', report_id)

    print '性别=========================================='
    hivesql = []
    hivesql.append('''
        select b.cont_name, a.uv, c.sum_uv
        from(
            select tags, uv
            from mixreport.tmp_table_profile_%(report_id)s
            where profile_type = 'dg_gender'
        )a
        join(
            select cont_id, cont_name
            from dmp_console.dim_cont
            where state = '1'
        )b
        on (a.tags = b.cont_id)
        join (
            select sum(uv) as sum_uv
            from mixreport.tmp_table_profile_%(report_id)s
            where profile_type = 'dg_gender'
        )c
    ''' %vars() )
    runHiveExe(hivesql,'gender_UV')
    updateTableValue('report', 'state', '25', 'report_id', report_id)

    #cont_path_UV
    print '兴趣=========================================='
    hivesql = []
    hivesql.append('''
        create table mixreport.tmp_table_cont_%(report_id)s as
        select tags, cont_path, count(1) as uv
        from(
            select t3.fa_cont_id as tags, t2.mix_uid, t4.cont_path
            from(
                select mix_uid
                from mixreport.dmp_srv_audience_data
                where audienceID in (%(audienceID)s)
                group by mix_uid
            )t1
            join(
               select mix_uid, tags
               from dmp_online.dmp_ud_user_profile_dt
               where profile_type = 'cont'
               and user_type = 'mobile' and tags like '8%%' and %(condition)s
               group by mix_uid, tags
            )t2
            on (t1.mix_uid = t2.mix_uid)
            join(
                select cont_id, fa_cont_id
                from dmp_console.dim_cont
                lateral view explode(split(cont_id_path,'/')) adtable as fa_cont_id
                where state = '1' and fa_cont_id != '80005' and fa_cont_id != '80371' and (cont_id < '82952' or cont_id > '82961')
            )t3
            on (t2.tags = t3.cont_id)
            join(
                select cont_id, cont_path
                from dmp_console.dim_cont
            )t4
            on (t3.fa_cont_id = t4.cont_id)
            group by t3.fa_cont_id, t2.mix_uid, t4.cont_path
        )t
        group by tags, cont_path
        ''' %vars() )

    HiveExe(hivesql)

    hivesql = []
    hivesql.append('''
        select b.cont_name, a.uv
        from(
            select tags, uv
            from mixreport.tmp_table_cont_%(report_id)s
            order by uv desc
            limit %(limit_num)s
        )a
        join(
            select cont_id, cont_name
            from dmp_console.dim_cont
            where state = '1'
        )b
        on (a.tags = b.cont_id)
    '''  % vars() )

    runHiveExe(hivesql,'cont_UV')
    updateTableValue('report', 'state', '30', 'report_id', report_id)

# ==============================
# 设备：dev_brand, dev_os, dev_type
# ==============================
    hivesql = []
    hivesql.append('''
        create table mixreport.tmp_table_dev_%(report_id)s as
        select x2.tags, count(1) as uv
        from(
            select mix_uid
            from mixreport.dmp_srv_audience_data
            where audienceID in (%(audienceID)s)
            group by mix_uid
        )x1
        join(
            select mix_uid, tags
            from dmp_online.dmp_ud_user_profile_dt
            where profile_type = 'dev' and user_type = 'mobile' and %(condition)s and tags like '403%%'
            group by mix_uid, tags
        )x2
        on(x1.mix_uid = x2.mix_uid)
        group by x2.tags
    ''' % vars() )

    HiveExe(hivesql)

    print '设备品牌=========================================='
    hivesql = []
    hivesql.append('''
        select b.brand_name, sum(a.uv) as v, c.uuv
        from(
            select tags, uv
            from mixreport.tmp_table_dev_%(report_id)s
        )a
        join(
            select model_id, brand_name
            from dmp_console.dim_data_device_model_base
            where state = '1' and brand_name != 'NULL' and brand_name != '' and device_os != 'NULL' and device_type != 'NULL'
        )b
        on(substr(a.tags,4) = b.model_id)
        join(
            select sum(a1.uv) as uuv
            from(
                select tags, uv
                from mixreport.tmp_table_dev_%(report_id)s
            )a1
            join(
                select model_id
                from dmp_console.dim_data_device_model_base
                where state = '1' and brand_name != 'NULL' and brand_name != '' and device_os != 'NULL' and device_type != 'NULL'
            )b1
            on (substr(a1.tags,4) = b1.model_id)
        )c
        group by b.brand_name, c.uuv
        order by v desc
        limit %(limit_num)s
    '''% vars())

    runHiveExe(hivesql,'dev_brand_UV')
    updateTableValue('report', 'state', '35', 'report_id', report_id)


    print '设备操作系统=========================================='
    hivesql = []
    hivesql.append('''
        select b.device_os, sum(a.uv) as v, c.uuv
        from(
            select tags, uv
            from mixreport.tmp_table_dev_%(report_id)s
        )a
        join(
            select model_id, device_os
            from dmp_console.dim_data_device_model_base
            where state = '1' and brand_name != 'NULL' and device_os != 'NULL' and device_type != 'NULL'
        )b
        on(substr(a.tags,4) = b.model_id)
        join(
            select sum(a1.uv) as uuv
            from(
                select tags, uv
                from mixreport.tmp_table_dev_%(report_id)s
            )a1
            join(
                select model_id
                from dmp_console.dim_data_device_model_base
                where state = '1' and brand_name != 'NULL' and device_os != 'NULL' and device_type != 'NULL'
            )b1
            on (substr(a1.tags,4) = b1.model_id)
        )c
        group by b.device_os, c.uuv
        order by v desc
        limit %(limit_num)s
    '''% vars())

    runHiveExe(hivesql,'dev_os_UV')
    updateTableValue('report', 'state', '40', 'report_id', report_id)

    print '设备类型=========================================='
    hivesql = []
    hivesql.append('''
        select b.device_type, sum(a.uv) as v, c.uuv
        from(
            select tags, uv
            from mixreport.tmp_table_dev_%(report_id)s
        )a
        join(
            select model_id, device_type
            from dmp_console.dim_data_device_model_base
            where state = '1' and brand_name != 'NULL' and device_os != 'NULL' and device_type != 'NULL'
        )b
        on(substr(a.tags,4) = b.model_id)
        join(
            select sum(a1.uv) as uuv
            from(
                select tags, uv
                from mixreport.tmp_table_dev_%(report_id)s
            )a1
            join(
                select model_id
                from dmp_console.dim_data_device_model_base
                where state = '1' and brand_name != 'NULL' and device_os != 'NULL' and device_type != 'NULL'
            )b1
            on (substr(a1.tags,4) = b1.model_id)
        )c
        group by b.device_type, c.uuv
        order by v desc
        limit %(limit_num)s
    '''% vars())

    runHiveExe(hivesql,'dev_type_UV')
    updateTableValue('report', 'state', '45', 'report_id', report_id)

# =====================================
# 行为: app, site, kw
# =====================================

    print '用户行为=========================================='
    hivesql = []
    hivesql.append('''
        create table mixreport.tmp_table_action_%(report_id)s as
        select t2.type_id, t2.tags, count(1) as uv
        from(
            select mix_uid
                from mixreport.dmp_srv_audience_data
                where audienceID in (%(audienceID)s)
                group by mix_uid
        )t1
        join(
           select mix_uid, type_id, tags
           from dmp_online.dmp_ud_user_stdtags_dt
           where type_id in ('b','c','d') and user_type = 'mobile' and %(condition)s
           group by mix_uid, type_id, tags
        )t2
        on (t1.mix_uid = t2.mix_uid)
        group by t2.type_id, t2.tags
    '''  % vars() )

    HiveExe(hivesql)

    print '网站=========================================='
    hivesql = []
    hivesql.append('''
        select b.domain, a.uv
        from(
            select tags, uv
            from mixreport.tmp_table_action_%(report_id)s
            where type_id = 'b'
            order by uv desc
            limit %(limit_num)s
        )a
        join(
            select site_id, domain
            from dmp_console.dim_site
            where state = '1'
        )b
        on (a.tags = b.site_id)
    '''  % vars() )

    runHiveExe(hivesql,'site_UV')
    updateTableValue('report', 'state', '50', 'report_id', report_id)

    print 'APP=========================================='
    hivesql = []
    hivesql.append('''
        select b.app_name , a.uv
        from(
            select tags, uv
            from mixreport.tmp_table_action_%(report_id)s
            where type_id = 'c'
            order by uv desc
            limit %(limit_num)s
        )a
        join(
            select app_id, app_name
            from dmp_console.dim_app
            where state = '1'
        )b
        on (a.tags = b.app_id)
    '''  % vars() )

    runHiveExe(hivesql,'app_UV')
    updateTableValue('report', 'state', '55', 'report_id', report_id)

    print '关键词=========================================='
    hivesql = []
    hivesql.append('''
        select b.kw, a.uv
        from(
            select tags, uv
            from mixreport.tmp_table_action_%(report_id)s
            where type_id = 'd'
            order by uv desc
            limit %(limit_num)s 
        )a
        join(
            select kwtag_id, kw
            from dmp_console.dim_data_kwtag_base
            where state = '1'
        )b
        on (a.tags = b.kwtag_id)
    '''  % vars() )

    runHiveExe(hivesql,'kw_UV')
    updateTableValue('report', 'state', '60', 'report_id', report_id)


    print '公众号=========================================='
    hivesql = []
    hivesql.append('''
        create table mixreport.tmp_table_pubnum_%(report_id)s as
        select t3.name, count(1) as uv
        from(
            select mix_uid
                from mixreport.dmp_srv_audience_data
                where audienceID in (%(audienceID)s)
                group by mix_uid
        )t1
        join(
            select mix_uid, relobj_id as tags
            from dmp_online.dmp_um_user_relobj_dt
            where user_type = 'mobile' and relobj_type = 'sladuid_pubnum' and %(condition)s
            group by mix_uid, relobj_id
        )t2
        on (t1.mix_uid = t2.mix_uid)
        join(
            select id, name
            from dmp_console.dim_data_pubnum_base
            where name != ''
        )t3
        on (t2.tags = SladuidEncode('18',t3.id))
        group by t3.name
    ''' % vars())
    HiveExe(hivesql)

    hivesql = []
    hivesql.append('''
        select name, uv
        from mixreport.tmp_table_pubnum_%(report_id)s
        order by uv desc
        limit %(limit_num)s 
    ''' % vars())
    runHiveExe(hivesql,'pubnum_UV')
    updateTableValue('report', 'state', '65', 'report_id', report_id)


# =================================
# TGI指标
# =================================

    #计算总uv
    hivesql = []
    hivesql.append('''
        select sum(uv) as sum_uv
        from mixreport.dim_tag_count
        where type = 'age'
    ''' % vars())

    runHiveExe(hivesql,'sum_UV')

    #app_tgi
    hivesql = []
    hivesql.append('''
        select c.app_name, (b.uv / %(audience_uv)s) / (a.uv / %(sum_uv)s) as tgi, b.uv
        from(
            select tags, uv
            from mixreport.dim_tag_count
            where type = 'app'
        )a
        join(
            select tags, uv
            from mixreport.tmp_table_action_%(report_id)s
            where type_id = 'c' and (uv / %(audience_uv)s) > %(tgi_bili)s  and uv > %(uv_min_num)s
        )b
        on (a.tags = b.tags)
        join(
            select app_id, app_name
            from dmp_console.dim_app
            where state = '1'
        )c
        on (a.tags = c.app_id)

        order by tgi desc
        limit %(limit_num)s

    ''' % vars())
    runHiveExe(hivesql,'app_TGI')

    updateTableValue('report', 'state', '70', 'report_id', report_id)

    #site_tgi
    hivesql = []
    hivesql.append('''
        select c.domain, (b.uv / %(audience_uv)s) / (a.uv / %(sum_uv)s) as tgi, b.uv
        from(
            select tags, uv
            from mixreport.dim_tag_count
            where type = 'site'
        )a
        join(
            select tags, uv
            from mixreport.tmp_table_action_%(report_id)s
            where type_id = 'b' and (uv / %(audience_uv)s) > %(tgi_bili)s and uv > %(uv_min_num)s
        )b
        on (a.tags = b.tags)
        join(
            select site_id, domain
            from dmp_console.dim_site
            where state = '1'
        )c
        on (a.tags = c.site_id)
        order by tgi desc
        limit %(limit_num)s

    ''' % vars())
    runHiveExe(hivesql,'site_TGI')
    updateTableValue('report', 'state', '75', 'report_id', report_id)


    #kw_tgi
    hivesql = []
    hivesql.append('''
        select c.kw, (b.uv / %(audience_uv)s) / (a.uv / %(sum_uv)s) as tgi
        from(
            select tags, uv
            from mixreport.dim_tag_count
            where type = 'kw'
        )a
        join(
            select tags, uv
            from mixreport.tmp_table_action_%(report_id)s
            where type_id = 'd' and (uv / %(audience_uv)s) > %(tgi_kw_bili)s and uv > %(uv_min_num)s
        )b
        on (a.tags = b.tags)
        join(
            select kwtag_id, kw
            from dmp_console.dim_data_kwtag_base
            where state = '1'
        )c
        on (a.tags = c.kwtag_id)
        order by tgi desc
        limit %(limit_num)s

    ''' % vars())
    runHiveExe(hivesql,'kw_TGI')

    updateTableValue('report', 'state', '80', 'report_id', report_id)

    #cont_UV_TGI
    hivesql = []
    hivesql.append('''
        select b.cont_name, b.cont_path, c.uv, (c.uv / %(audience_uv)s) / (a.uv / %(sum_uv)s) as tgi
        from(
            select tags, uv
            from mixreport.dim_tag_count
            where type = 'cont'
        )a
        join(
            select cont_id, cont_name, cont_path
            from dmp_console.dim_cont
            where state = '1'
        )b
        on (a.tags = b.cont_id)
        join(
            select tags, if(uv > %(uv_min_num)s and (uv / %(audience_uv)s) > %(tgi_cont_bili)s, uv, 0) as uv
            from mixreport.tmp_table_cont_%(report_id)s
        )c
        on (b.cont_id = c.tags)
        where c.uv > 0
        order by b.cont_path

    ''' % vars())
    runHiveExe(hivesql,'cont_UV_TGI')

    #updateTableValue('report', 'state', '90', 'report_id', report_id)

    #cont_tgi
    hivesql = []
    hivesql.append('''
        select b.cont_name, (c.uv / %(audience_uv)s) / (a.uv / %(sum_uv)s) as tgi, b.cont_path, c.uv
        from(
            select tags, uv
            from mixreport.dim_tag_count
            where type = 'cont'
        )a
        join(
            select cont_id, cont_name, cont_path
            from dmp_console.dim_cont
            where state = '1'
        )b
        on (a.tags = b.cont_id)
        join(
            select tags, uv
            from mixreport.tmp_table_cont_%(report_id)s
            where (uv / %(audience_uv)s) > %(tgi_cont_bili)s and uv > %(uv_min_num)s
        )c
        on (b.cont_id = c.tags)
        order by tgi desc
        limit %(limit_num)s

    ''' % vars())
    runHiveExe(hivesql,'cont_TGI')

    updateTableValue('report', 'state', '90', 'report_id', report_id)

    #pubnum_tgi
    hivesql = []
    hivesql.append('''
        select c.name, (d.uv / %(audience_uv)s) / (c.uv / %(sum_uv)s) as tgi, d.uv
        from(
            select b.name, sum(a.uv) as uv
            from(
                select tags, uv
                from mixreport.dim_tag_count
                where type = 'pubnum'
            )a
            join(
                select id, name
                from dmp_console.dim_data_pubnum_base
                where name != ''
            )b
            on (SladuidEncode('18',b.id) = a.tags)
            group by b.name
        )c
        join(
            select name, uv
            from(
                select name, sum(uv) as uv
                from mixreport.tmp_table_pubnum_%(report_id)s
                group by name
            )t
            where (uv / %(audience_uv)s) > %(tgi_pubnum_bili)s and uv > %(uv_min_num)s
        )d
        on (d.name = c.name)
        order by tgi desc
        limit %(limit_num)s

    ''' % vars())
    runHiveExe(hivesql,'pubnum_TGI')

    updateTableValue('report', 'state', '100', 'report_id', report_id)
    updateTableValue('report', 'is_run', '0', 'report_id', report_id)

# =================================
# 删除临时表
# =================================
    #Delete_Tmp_Table(report_id)

