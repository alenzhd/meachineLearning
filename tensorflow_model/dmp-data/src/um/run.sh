basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
net_type=$3
dates=$4

sh $basepath/../uc/run.sh $provider $province $net_type $dates D

python $basepath/dmp_um_id_rel_bd.py $provider $province $net_type $dates

python $basepath/dmp_um_user_dt.py $provider $province $net_type $dates

python $basepath/dmp_um_id_rel_dt.py $provider $province $net_type $dates

python $basepath/dmp_um_user_rel_dt.py $provider $province $net_type $dates

python $basepath/dmp_um_user_relobj_dt.py $provider $province $net_type $dates
