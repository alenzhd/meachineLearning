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


#开始执行通知
python $basepath/../../../../../src/tools/redis/redisNotice.py $provider 3 start $net_type $province $day
#用户管理
sh $basepath/../../../../../src/um/run.sh $provider $province $net_type $day
