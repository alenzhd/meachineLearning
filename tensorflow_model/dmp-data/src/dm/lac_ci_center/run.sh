basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
dates=$3

#lac_ci中心点发现
python $basepath/dmp_lac_ci_center.py $provider $province $dates
