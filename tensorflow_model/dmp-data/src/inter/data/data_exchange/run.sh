basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
dates=$3

#提供用户画像后的个人关注人群标签表
python $basepath/dmp_srv_user_tags.py $provider $province $dates
