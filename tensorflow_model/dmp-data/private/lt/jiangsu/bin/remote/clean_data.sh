#!/bin/sh
yyyymmdd=${1:0:8}
yyyymmdd_delay=`date --date="$yyyymmdd -1 day" +%Y%m%d`
yyyymmdd_delay_7d=`date --date="$yyyymmdd - 3 day" +%Y%m%d`
yyyymmdd_delay_1m=`date --date="$yyyymmdd - 7 day" +%Y%m%d`

hh=${1:8:2}
run_shell=$0
export COMMAND=${run_shell##*/}
export PWD=${run_shell%/*}

#参数检查
if [[ `expr length $yyyymmdd` -ne 8 ]]
then
		echo need parameter such as YYYYMMDDHH
		exit	3
fi
echo $PWD


hive -e "use dmp_yx;
alter table dmp_ci_m_bh drop partition(day_id='$yyyymmdd_delay');
alter table dmp_ci_track_m_bh drop partition(day_id='$yyyymmdd_delay_7d');
alter table dmp_ci_user_m_bh drop partition(day_id='$yyyymmdd_delay_1m');
alter table dmp_mn_kpi_bd drop partition(day_id='$yyyymmdd_delay_1m');
alter table dmp_mn_kpi_bh drop partition(day_id='$yyyymmdd_delay_1m');
alter table dmp_uc_otags_m_bh drop partition(day_id='$yyyymmdd_delay_1m');
alter table dmp_uc_tags_m_bh drop partition(day_id='$yyyymmdd_delay_1m');
alter table dmp_upload_inter_tab_bh drop partition(day_id='$yyyymmdd_delay_1m');
alter table dmp_upload_inter_tab_bh_1 drop partition(day_id='$yyyymmdd_delay_1m');
alter table dmp_upload_inter_tab_bh_2 drop partition(day_id='$yyyymmdd_delay_1m');

"
#清理回收站
hadoop fs -rm -r .Trash/*