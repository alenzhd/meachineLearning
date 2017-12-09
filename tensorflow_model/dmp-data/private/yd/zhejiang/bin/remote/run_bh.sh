basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
hour=$3

#内容识别
sh $basepath/../../../../src/ci/run.sh $provider $province $hour $hour

#小时级监控数据上传
python  $basepath/../../../../src/inter/kv/updownload/upload.py $provider $province $hour dmp_mn_kpi_bh
