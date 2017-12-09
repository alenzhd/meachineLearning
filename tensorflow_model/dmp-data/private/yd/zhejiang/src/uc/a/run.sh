basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
dates=$3
python $basepath/a_info_combine_bd.py $provider $province $dates

start_hour=${dates}00
end_hour=${dates}23
python $basepath/a_info_combine_bh.py $provider $province $start_hour $end_hour
