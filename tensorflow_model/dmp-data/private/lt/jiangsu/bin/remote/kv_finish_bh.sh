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
		exit	3
fi

echo -e "${hour}\0004finish" >>$PWD/flag/flag.${hour}

hadoop fs -mkdir -p /DMP_YX/hive/warehouse/dmp_yx.db/to_upkv/to_upkv_${yyyymmddhh}_flag/

hadoop fs -put -f $PWD/flag/flag.${hour} /DMP_YX/hive/warehouse/dmp_yx.db/to_upkv/to_upkv_${yyyymmddhh}_flag/

#hadoop fs -put -f $PWD/flag/flag.${huor} /DMP_YX/hive/warehouse/dmp_yx.db/to_upkv_flag/
