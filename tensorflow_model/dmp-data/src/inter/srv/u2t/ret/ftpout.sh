#!/bin/bash
source ~/.bash_profile

basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
net_type=$3
yyyymmdd=$4
flag=$5


python $basepath/check.py ${provider} ${province} ${net_type} ${yyyymmdd} u2t
if [ $? -eq 0 ];then
else
exit -1
fi

sql="use dmp_online;select * from dmp_srv_id_htags_bd where provider='$provider' and province='$province'and net_type='${net_type}' and day_id='$yyyymmdd' and flag='$flag'"
ret_dir=~/DATA/OUT/u2t/${yyyymmdd}
mkdir -p $ret_dir

if [ $flag == 'imei' ];then
flag1='md532_imei'
else
flag1=$flag
fi

hive -e "$sql"|awk -F ''$flag'' '{print $2}'|awk -F '_' '{print $2}'|sort -u > ${ret_dir}/${provider}-${province}-${net_type}-${flag1}.data

#python $basepath/check_ftp.py $provider $province ${net_type} $yyyymmdd $flag1 RET_U2T
#if [ $? -eq 0 ];then
#echo "写入通知!"
echo "finished" > ${ret_dir}/${provider}-${province}-${net_type}-${flag1}.notice
#else
#echo "检查异常!"
#fi

