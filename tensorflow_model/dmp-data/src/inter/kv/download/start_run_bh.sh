#!/bin/bash
yyyymmddhh=`date --date="-2 hour" +%Y%m%d%H`
nohup sh ./run_bh.sh $yyyymmddhh > logs/run_bh_$yyyymmddhh.log 2>&1 &

