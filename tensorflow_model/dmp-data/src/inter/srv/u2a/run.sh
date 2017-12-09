#!/bin/bash

basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
net_type=$3
dates=$4
#需要上传的表名，目前为 dmp_srv_adrel_bd
table_name='dmp_srv_adrel_bd'

python $basepath/dmp_srv_adrel_bd.py $provider $province $net_type $dates

# adrel 标签数据上传到服务
python $basepath/dmp_srv_upload_bd.py $provider $province $net_type $dates $table_name

# 检查
python $basepath/../../../tools/cop/check_hdfs.py $provider $province $net_type $dates SRV_U2A
if [ $? -eq 0 ];then
echo "检查通过!"
else
echo "检查异常!"
exit -1
fi
