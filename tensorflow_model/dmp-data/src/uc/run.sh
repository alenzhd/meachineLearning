basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
net_type=$3
day=$4
action=$5
if [[ $provider == "lt" && $province == "jiangsu" ]];then 
python $basepath/dmp_uc_tags_m_bd.py $provider $province $net_type $day
fi
python $basepath/dmp_uc.py $provider $province $net_type $day $action

