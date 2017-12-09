#!/bin/bash
basepath=$(cd `dirname $0`; pwd)

provider='dxy'
province='beijing'
net_type='mobile'
dates=$1

#每天23点调用
#参数除了dates,其他为固定参数(为了能够兼容settings.py，实际上脚本中这几个参数是没有用的)
python $basepath/dmp_srv_idmapping_bd.py $provider $province $net_type $dates
## 说明:固定值是不用改变


#需要上传的表名，这里为 dmp_srv_idmapping_bd
table_name='dmp_srv_idmapping_bd'
#table_name='dmp_srv_idmapping_dt'

# idmapping 标签数据上传到服务
python -u $basepath/dmp_srv_idmapping_upload_bd.py $provider $province $net_type $dates $table_name

