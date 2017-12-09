#!/bin/sh
day_id=$1
run_shell=$0
export COMMAND=${run_shell##*/}
export PWD=${run_shell%/*}

cd $PWD
java -jar $PWD/dmp-download.jar action=download tableName=dmp_mn_kpi_bd province=jiangsu provider=lt netType=mobile startTime=$day_id endTime=$day_id
cd -

cp /data1/user/dmptest/dmp3/download/data/suc/dmp_mn_kpi_bd_*_${day_id}.txt /data1/user/dmptest/dmp3/data/mn/

