#!/bin/sh


hour_id=${1}00
pri=$2
run_shell=$0
export COMMAND=${run_shell##*/}
export PWD=${run_shell%/*}

cd $PWD
echo $hour_id
sh $PWD/run.sh action=download pri=${pri} tableName=dmp_upload_inter_tab_bh_tmp province=jiangsu startTime=${hour_id} endTime=${hour_id} provider=lt netType=mobile
cd -

