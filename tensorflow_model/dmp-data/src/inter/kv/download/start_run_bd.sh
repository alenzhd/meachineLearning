#!/bin/bash
yyyymmdd=`date --date="-1 day" +%Y%m%d`
nohup sh ./run_bd.sh $yyyymmdd > logs/run_bh_$yyyymmdd.log 2>&1 &

