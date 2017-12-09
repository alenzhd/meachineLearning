#coding=utf-8
import sys,subprocess,redis,time

filepath = sys.argv[1]

host="10.1.3.216"
port="6379"
separator='\t'


v_len=len(sys.argv)
if v_len==2:
    v_time="0"
elif v_len==3:
    v_time=sys.argv[2]
else:
    print "参数异常！"
    sys.exit(-1)

if v_time=="0":
    print "有效期:永不失效！"
else:
    print "有效期:"+v_time+"秒."

r = redis.Redis(host, port, db=0)

start = time.time()

try:
    cat = subprocess.Popen(["hadoop", "fs", "-text", filepath],stdout=subprocess.PIPE)
    count=0
    for line in cat.stdout:
        s=line.strip().split(separator)
        key=s[0]
        value=s[1]
        if v_time=="0":
          r.set(key,value)
        elif v_time>"0":
          r.setex(key, str(value), v_time)
        else :
          print "参数异常！"
          sys.exit(-1)
          
        #print s[0]+":"+s[1]
        count+=1
        if count%100==0:
            print "----->正在上传:"+str(count)

    print "上传完成,总量:"+str(count)

except Exception,e:
    print Exception,":",e

end = time.time()
print "耗时:" + str(end - start)
