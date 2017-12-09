#!/bin/sh
yyyymmddhh=`date --date="-3 day" +%Y%m%d%H`

run_shell=$0
export COMMAND=${run_shell##*/}
export PWD=${run_shell%/*}

#参数检查
if [[ `expr length ${yyyymmddhh}` -ne 10 ]]
then
                echo need parameter such as YYYYMMDDHH
                exit 3
fi
echo $PWD

hive -e "use dmp_yx;
drop table to_upkv_${yyyymmddhh};
drop table to_upkv_${yyyymmddhh};
"
