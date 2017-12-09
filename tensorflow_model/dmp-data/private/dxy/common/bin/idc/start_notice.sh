#!/bin/bash
source ~/.bash_profile

basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
net_type=$3
day=$4

#通知用户管理的接口
python $basepath/../../../../../src/tools/notice/schedule.py $provider $province $net_type $day 

#开始执行通知
python $basepath/../../../../../src/tools/redis/redisNotice.py $provider 3 start $net_type $province $day
