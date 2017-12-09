basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
dates=$3

#echo "######用户管理,查看日流程######
sh $basepath/../../../../src/um/run.sh $provider $province $dates


#lac ci中心点(目前用户画像没有使用该模块数据)
sh $basepath/../../../../src/dm/lac_ci_center/run.sh $provider $province $dates

#A口信令汇总(目前用户画像没有使用该模块数据)
sh $basepath/../src/uc/a/run.sh $provider $province $dates

#用户画像
sh $basepath/../../../../src/ud/ul/run.sh $provider $province $dates
#用户画像--生成imei/idfa等用户的标签数据
#python $basepath/../src/ud/ul/dmp_ud_ul_htags_m_bd.py $dates

#按天监控
sh $basepath/../../../../src/mn/run.sh $provider $province $dates

#天级监控数据上传
python  $basepath/../../../../src/inter/kv/updownload/upload.py $provider $province $dates dmp_mn_kpi_bd
#python  $basepath/../../../../src/inter/kv/updownload/upload.py $provider $province $dates dmp_ud_ul_htags_m_bd


