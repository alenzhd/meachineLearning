yyyymmdd=${1:0:8}

hive -e "
use dmp_online;
set hive.exec.compress.output=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dmp_uc_otags_m_bh partition(provider,province,day_id,hour_id,sa_id)
select mix_m_uid,tag_index,duration,last_timestamp,count,provider,province,substr(hour,0,8) as day_id,hour as hour_id,sa_id from dmp_uc_otags_m_bh_jslt_tmp where day_id=${yyyymmdd};
"

hive -e "
use dmp_online;
set hive.exec.compress.output=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dmp_uc_tags_m_bh partition(provider,province,day_id,hour_id)
select mix_m_uid,app_id,site_id,cont_id,action_id,value,value_type_id,duration,last_timestamp,count,provider,province,substr(hour,0,8) as day_id,hour as hour_id from dmp_uc_tags_m_bh_jslt_tmp where day_id=${yyyymmdd}
"

hive -e "
use dmp_online;
set hive.exec.compress.output=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dmp_um_m_ht partition(provider,province,day_id,hour_id,mflag)
select mix_m_uid,m_id,id2uid_score,uid2id_score,provider,province,substr(hour,0,8) as day_id,hour as hour_id,mflag from dmp_um_m_ht_jslt_tmp where day_id=${yyyymmdd};
"
