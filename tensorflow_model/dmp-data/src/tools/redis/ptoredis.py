# -*- coding: utf-8 -*-
import os,time,sys,redis
import threading

host="10.1.3.208"
port=6379
separator='\t'


rlock = threading.RLock()
curPosition = 0

class Reader(threading.Thread):
    def __init__(self, res):
        self.res = res
        super(Reader, self).__init__()
    def run(self):
        global count,curPosition
        fstream = open(self.res.fileName, 'r')
        while True:
            #锁定共享资源
            rlock.acquire()
            startPosition = curPosition
            curPosition = endPosition = (startPosition + self.res.blockSize) if (startPosition + self.res.blockSize) < self.res.fileSize else self.res.fileSize
            #释放共享资源
            rlock.release()
            if startPosition == self.res.fileSize:
                break
            elif startPosition != 0:
                fstream.seek(startPosition)
                fstream.readline()
            pos = fstream.tell()
            while pos < endPosition:
                line = fstream.readline()
                #处理line
                #print(line.strip())
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
                    print "------->正在上传至redis:"+str(count)+"("+threading.currentThread().getName()+")"
                    sys.stdout.flush()
                pos = fstream.tell()
        fstream.close()

class Resource(object):
    def __init__(self, fileName):
        self.fileName = fileName
        #分块大小
        self.blockSize = 100000000
        self.getFileSize()
    #计算文件大小
    def getFileSize(self):
        fstream = open(self.fileName, 'r')
        fstream.seek(0, os.SEEK_END)
        self.fileSize = fstream.tell()
        fstream.close()

if __name__ == '__main__':
    hdfspath=sys.argv[1]
    starttime = time.clock()
    count=0 
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

    os.system("hadoop fs -text "+hdfspath+" > hdfsdata.txt")
    #线程数
    threadNum = 20
    #文件
    fileName = 'hdfsdata.txt';
    res = Resource(fileName)
    threads = []
    #初始化线程
    for i in range(threadNum):
        rdr = Reader(res)
        threads.append(rdr)
    #开始线程
    for i in range(threadNum):
        threads[i].start()
    #结束线程
    for i in range(threadNum):
        threads[i].join()
    print "上传完成,总量:"+str(count)

    print(time.clock() - starttime)
