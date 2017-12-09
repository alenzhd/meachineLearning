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
action="I"

#开始用户汇总通知
python $basepath/../../../../../src/tools/redis/redisNotice.py $provider 2 start $net_type $province $day
#用户汇总
sh $basepath/../../../../../src/uc/run.sh $provider $province $net_type $day $action
#结束用户汇总通知
python $basepath/../../../../../src/tools/redis/redisNotice.py $provider 2 end $net_type $province $day

#开始执行通知
python $basepath/../../../../../src/tools/redis/redisNotice.py $provider 3 start $net_type $province $day
#用户管理
sh $basepath/../../../../../src/um/run.sh $provider $province $net_type $day
#用户画像
sh $basepath/../../../../../src/ud/run.sh $provider $province $net_type $day
#标签服务
sh $basepath/../../../../../src/inter/srv/u2t/run.sh $provider $province $net_type $day
#跨屏服务（移网暂无）
#sh $basepath/../../../../../src/inter/srv/u2a/run.sh $provider $province $net_type $day
#结束执行通知
python $basepath/../../../../../src/tools/redis/redisNotice.py $provider 3 end $net_type $province $day
