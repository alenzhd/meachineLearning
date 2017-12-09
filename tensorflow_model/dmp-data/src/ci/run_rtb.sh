basepath=$(cd `dirname $0`; pwd)
#provider=$1
#province=$2
#nettype=$3
day_id=$1
python $basepath/dmp_rtb-ci.py dxy beijing mobile $day_id
