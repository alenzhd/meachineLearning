#!/bin/bash

basepath=$(cd `dirname $0`; pwd)

yyyymmdd=`date --date="-1 day" +"%Y%m%d"`
#yyyymmdd=$1

adsl_provinces="shanghai jiangsu zhejiang anhui fujian shandong guangdong sichuan chongqing hainan"
for province in $adsl_provinces
do 

flag='idfa'
nohup sh $basepath/ftpout.sh dxy $province adsl $yyyymmdd $flag > $basepath/logs/dxy_${province}_${yyyymmdd}_${flag}.log 2>&1&
sleep 10


flag='imei'
nohup sh $basepath/ftpout.sh dxy $province adsl $yyyymmdd $flag > $basepath/logs/dxy_${province}_${yyyymmdd}_${flag}.log 2>&1&


sleep 60
done
