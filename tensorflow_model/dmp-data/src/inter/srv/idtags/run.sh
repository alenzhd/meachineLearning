#!/bin/bash

basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
net_type=$3
dates=$4

#只用到了日期参数
python $basepath/dmp_srv_id_tags_bd.py 'dxy' 'beijing' 'mobile' $dates