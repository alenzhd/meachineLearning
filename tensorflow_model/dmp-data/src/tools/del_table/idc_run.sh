basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
net_type=$3
dates=$4

#删除表
python $basepath/idc_del_table.py $provider $province $net_type $dates