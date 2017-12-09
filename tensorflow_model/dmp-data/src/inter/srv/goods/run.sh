basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
dates=$3

#更新对外提供服务的数据（更新redis，供前台查询）
python $basepath/dmp_srv_toredis_bd.py $provider $province $dates
