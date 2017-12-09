#!/bin/sh
hour_id=$1
day_id=${1:0:8}
run_shell=$0
export COMMAND=${run_shell##*/}
export PWD=${run_shell%/*}

cd $PWD

hive -e "
use dmp_online;
load data local inpath '/data1/user/dmp/download/data/suc/dmp_upload_inter_tab_bh_tmp_lt_jiangsu_mobile_${hour_id}_*_mot' 
overwrite into table dmp_uc_otags_m_bh_jslt_tmp partition(provider='lt',province='jiangsu',day_id='${day_id}',hour_id='${hour_id}');

load data local inpath '/data1/user/dmp/download/data/suc/dmp_upload_inter_tab_bh_tmp_lt_jiangsu_mobile_${hour_id}_*_mt'
overwrite into table dmp_uc_tags_m_bh_jslt_tmp partition(provider='lt',province='jiangsu',day_id='${day_id}',hour_id='${hour_id}');

load data local inpath '/data1/user/dmp/download/data/suc/dmp_upload_inter_tab_bh_tmp_lt_jiangsu_mobile_${hour_id}_*_um'
overwrite into table dmp_um_m_ht_jslt_tmp partition(provider='lt',province='jiangsu',day_id='${day_id}',hour_id='${hour_id}');

"

cd -

