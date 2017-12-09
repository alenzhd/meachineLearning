basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
net_type=$3
dates=$4

python $basepath/dmp_ud_id_tags_bd.py $provider $province $net_type $dates

sh $basepath/run_tmp.sh $provider $province $net_type $dates > ~/logs/${dates}/run_tmp_${provider}_${province}_${net_type}_${dates}

python $basepath/../tools/cop/check_hdfs.py $provider $province $net_type $dates UMUD
if [ $? -eq 0 ];then
echo "检查通过!"
else
echo "检查异常!"
exit -1
fi

