#coding=utf-8
import sys,redis,time
providers={"dxy":"dxy","lt":"lt","yd":"yd"}
processes={"1":"d","2":"p","3":"u","u2t":"u2t","u2a":"u2a"}
states={"start":"s","end":"e"}
nettypes={"adsl":"a","mobile":"m"}
provinces={"beijing":"11","tianjin":"12","hebei":"13","shanxi":"14","neimenggu":"15",
"liaoning":"21","jilin":"22","heilongjiang":"23","shanghai":"31","jiangsu":"32",
"zhejiang":"33","anhui":"34","fujian":"35","jiangxi":"36","shandong":"37","henan":"41",
"hubei":"42","hunan":"43","guangdong":"44","guangxi":"45","hainan":"46","chongqing":"50",
"sichuan":"51","guizhou":"52","yunnan":"53","xizang":"54","shanxisheng":"61","gansu":"62",
"qinghai":"63","ningxia":"64","xinjiang":"65"}

key=providers[sys.argv[1]]+"_"+processes[sys.argv[2]]+"_"+states[sys.argv[3]]+"_"+nettypes[sys.argv[4]]+"_"+provinces[sys.argv[5]]+"_"+sys.argv[6]
value = time.strftime("%Y%m%d%H%M%S",time.localtime(time.time()))
print key+"\t"+value
host="10.1.1.100"
port=6399
r = redis.Redis(host, port, db=0)
r.set(key,value)
