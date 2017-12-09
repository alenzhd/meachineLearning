#!/bin/bash
source ~/.bash_profile
basepath=$(cd `dirname $0`; pwd)

logbasedir="/data11/dacp/cszhaoyf/logs"

yyyymmddhh='2016040823'
yyyymmdd=${yyyymmddhh:0:8}
logdir=$logbasedir/${yyyymmdd}
mkdir -p $logdir
#固网运行省份调度
adsl_provinces=""
for province in $adsl_provinces
do 
sh $basepath/run_bh.sh dxy $province adsl $yyyymmddhh 2>&1 |tee $logdir/adsl_${province}_${yyyymmddhh}.log & sleep 60
done

yyyymmddhh='2016040616'
yyyymmdd=${yyyymmddhh:0:8}
logdir=$logbasedir/${yyyymmdd}
mkdir -p $logdir
#移网运行省份调度
mobile_provinces="jiangsu"
for province in $mobile_provinces
do 
sh $basepath/run_bh.sh dxy $province mobile $yyyymmddhh $logbasedir 2>&1 |tee $logdir/mobile_${province}_${yyyymmddhh}.log & sleep 60
done

wait 
exit 0