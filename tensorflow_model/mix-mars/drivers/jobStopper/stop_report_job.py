#-*- coding:utf-8 -*-
'''
Created on 2017年6月14日

@author: Administrator
'''

import sys
import subprocess
import time
import re

reload(sys)
sys.setdefaultencoding('utf-8')

ISOTIMEFORMAT='%Y-%m-%d %X'

KILL_MIX_AUDIENCE_SHELL_JOB='''ps -ef | \
grep -E 'create_mix_audience.py .* %(job_id)s|create_mix_audience.sh .* %(job_id)s' | \
grep -v 'grep -E' | \
awk '{print $2}' | \
xargs -n 1 kill -9
'''

KILL_REPORT_SHELL_JOB='''ps -ef | \
grep -E 'create_report.py .* %(job_id)s|create_report.sh .* %(job_id)s' | \
grep -v 'grep -E' | \
awk '{print $2}' | \
xargs -n 1 kill -9
'''

KILL_USER_AUDIENCE_SHELL_JOB='''ps -ef | \
grep -E 'create_user_audience.py .* %(job_id)s|create_user_audience.sh .* %(job_id)s' | \
grep -v 'grep -E' | \
awk '{print $2}' | \
xargs -n 1 kill -9
'''

KILL_YARN_APPLICATION='''yarn application -kill %(job_id)s
'''

start_job_pattern=re.compile(r'Starting Job = (job_.*?), Tracking URL = .*')
end_job_pattern=re.compile(r'Ended Job = (job_.*)')

def ShellCmdExe(cmd):
    "执行指定的shell命令"
    maxCount = 3  #最大重试次数
    status = 1
    count = 1
    std_out = ""
    while status != 0 and count <= maxCount:
        print "StartHdfs:%s" % time.strftime( ISOTIMEFORMAT, time.localtime() )
        print "------cmd------%s\n" % cmd
        try:
            std_out = subprocess.check_output(cmd, shell=True)
            print 'std_out='+std_out
            print "EndHdfs:%s" % time.strftime( ISOTIMEFORMAT, time.localtime() )
            return 0, std_out
        except subprocess.CalledProcessError, e:
            tmpLog = '第'+str(count)+'执行错误，重新执行！'
            print tmpLog
            count = count + 1
            print e
    else:
        tmpLog = '执行次数超过最大重试次数【'+str(maxCount)+'】，退出执行！'
        print tmpLog
        return 1, std_out

def stopShellJobs(job_id, flag):
    "终止创建人群和报告的脚本job"
    if(flag.strip() == 'mix'):
        cmd = KILL_MIX_AUDIENCE_SHELL_JOB % {'job_id':job_id}
    elif(flag.strip() == 'user'):
        cmd = KILL_USER_AUDIENCE_SHELL_JOB % {'job_id':job_id}
    elif(flag.strip() == 'report'):
        cmd = KILL_REPORT_SHELL_JOB % {'job_id':job_id}
    else:
        print '请确认将要终止的任务类型：mix/user/report', 'job_id='+job_id, 'flag='+flag
    print '将要执行的终止任务命令：'+cmd
    ShellCmdExe(cmd)

def stopHadoopJobs(job_id, flag):
    "终止创建人群和报告的hadoop job"
    if(flag.strip() == 'mix' or flag.strip() == 'user'):
        log_file='./logs/audience/'+job_id+'.log'
    elif(flag.strip() == 'report'):
        log_file='./logs/report/'+job_id+'.log'
    else:
        print '未找到需要终止任务的日志文件：', 'job_id='+job_id, 'flag='+flag
        
    job_id_list = getJobIdFromFile(log_file)
    for jd in job_id_list:
        cmd = KILL_YARN_APPLICATION % {'job_id':jd.replace('job', 'application')}
        print '将要执行的终止hadoop任务命令：'+cmd
        ShellCmdExe(cmd)
        
def getJobIdFromFile(log_file):
    "从日志文件中获取未结束的hadoop job id"
    job_id_list = []
#     end_job_id_lis = []
    with open(log_file, 'r') as f:
        for line in f.readlines():
            start_match_obj=start_job_pattern.match(line)
            end_match_obj=end_job_pattern.match(line)
            if start_match_obj:
                start_job_id=start_match_obj.group(1).strip()
                print 'start job id:', start_job_id
                job_id_list.append(start_job_id)
            elif end_match_obj:
                end_job_id=end_match_obj.group(1).strip()
                print 'end job id:',end_job_id
                if end_job_id in job_id_list:
                    job_id_list.remove(end_job_id)
                else:
                    print '不存在start job id:', end_job_id
    print 'job_id_list: ', ' '.join(job_id_list)
    return job_id_list

if __name__ == '__main__':
    args = sys.argv
    if len(args) == 3:
        stopShellJobs(args[1], args[2])
        stopHadoopJobs(args[1], args[2])
    else:
        print '参数输入错误：stop_report_job.py audience_id/report_id mix/user/report'
                
                
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            