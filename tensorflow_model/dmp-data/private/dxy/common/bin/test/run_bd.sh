#!/bin/bash
source ~/.bash_profile

basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
net_type=$3
day=$4
action=$5

#用户汇总
sh $basepath/../../../../../src/uc/run.sh $provider $province $net_type $day $action

#删除过期的分区
sh $basepath/../../../../../src/tools/del_table/run.sh $provider $province $net_type $day
