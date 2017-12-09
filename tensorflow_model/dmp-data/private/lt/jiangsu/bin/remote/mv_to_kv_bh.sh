#!/bin/sh
hour=$1
yyyymm=${1:0:6}
yyyymmdd=${1:0:8}
yyyymmddhh=${1}
hh=${1:8:2}
run_shell=$0
export COMMAND=${run_shell##*/}
export PWD=${run_shell%/*}

#参数检查
if [[ `expr length $hour` -ne 10 ]]
then
		echo need parameter such as YYYYMMDDHH
		exit 3
fi

hive -e "use dmp_yx;
CREATE	TABLE if not exists to_upkv_${yyyymmddhh}(
  key string,
  value string
)ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u0004'
STORED AS TextFile
location '/DMP_YX/hive/warehouse/dmp_yx.db/to_upkv/to_upkv_${yyyymmddhh}';
insert overwrite table to_upkv_${yyyymmddhh}
select key,value from dmp_upload_inter_tab_bh where day_id=$yyyymmdd and hour_id=$yyyymmddhh
"
