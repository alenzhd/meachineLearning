#!/bin/bash
source ~/.bash_profile

basepath=$(cd `dirname $0`; pwd)
yyyymmddhh=$1
hh=${1:8:2}
dates=${1:0:8}

if [ $hh -ne '23' ];
then
echo "未到23点，程序退出！"
exit 0
fi

provider='lt'
province='jiangsu'
net_type='mobile'
day=$dates


#用户画像
sh $basepath/../../../../../src/ud/run.sh $provider $province $net_type $day
if [ $? -eq 0 ];then
echo "检查通过!"
else
echo "检查异常!"
exit -1
fi
