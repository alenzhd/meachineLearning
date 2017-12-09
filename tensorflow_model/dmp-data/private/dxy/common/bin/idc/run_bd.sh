#!/bin/bash
source ~/.bash_profile

basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
net_type=$3
day=$4

#用户解析完成通知
python $basepath/../../../../../src/tools/notice/schedule.py $provider $province $net_type $day 

#开始执行通知
python $basepath/../../../../../src/tools/redis/redisNotice.py $provider 3 start $net_type $province $day
#用户管理
sh $basepath/../../../../../src/um/run.sh $provider $province $net_type $day
#用户画像
sh $basepath/../../../../../src/ud/run.sh $provider $province $net_type $day
#标签服务
sh $basepath/../../../../../src/inter/srv/u2t/run.sh $provider $province $net_type $day
#跨屏服务
sh $basepath/../../../../../src/inter/srv/u2a/run.sh $provider $province $net_type $day
#结束执行通知
python $basepath/../../../../../src/tools/redis/redisNotice.py $provider 3 end $net_type $province $day