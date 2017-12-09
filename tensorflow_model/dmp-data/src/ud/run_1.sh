basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
net_type=$3
dates=$4

python $basepath/dmp_ud_id_tags_bd.py $provider $province $net_type $dates

