#!/bin/sh
hour_id=$1
run_shell=$0
export COMMAND=${run_shell##*/}
export PWD=${run_shell%/*}

cd $PWD
java -jar $PWD/dmp-download.jar action=download tableName=dmp_mn_kpi_bh provider=lt province=jiangsu netType=mobile startTime=$hour_id endTime=$hour_id
cd -

cp /data1/user/dmptest/dmp3/download/data/suc/dmp_mn_kpi_bh_*_${hour_id}.txt /data1/user/dmptest/dmp3/data/mn/

