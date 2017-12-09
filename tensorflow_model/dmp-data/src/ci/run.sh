basepath=$(cd `dirname $0`; pwd)

provider=$1
province=$2
nettype=$3
start_hour=$4
end_hour=$5
jobName=job1job2
python $basepath/dmp_ci.py $provider $province $nettype $start_hour $end_hour $jobName
