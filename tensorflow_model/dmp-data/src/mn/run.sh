basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
dates=$3

#用户汇总监控数据
python $basepath/uc/user_tag_monitor.py $provider $province $dates
python $basepath/uc/user_tag_avg_monitor.py  $provider $province  $dates

#用户管理监控数据
python $basepath/um/user_monitor.py  $provider $province $dates

#用户画像监控数据
python $basepath/ud/ud_monitor.py  $provider $province $dates


