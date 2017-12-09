import sys,os,string
import traceback

if __name__ == '__main__':
    last_name = ""
    last_value = ""
    last_time = 0
    last_count = ""
    map = dict()
    for line in sys.stdin:
        try:
            temp = line.strip().split("\t")
            mixuid = temp[0]
            value = temp[1]
            count = temp[2]
            time_stamp = temp[3]
            day_id = temp[4]
            if not count.isdigit():
                continue
            time = long(temp[3])
            if last_name == mixuid:
                if last_name.strip() not in value.strip() and (time-last_time)/1000 > 10:
                    s = map.get(mixuid, dict())
                    m = s.get(last_value, 0)
                    m += int(last_count)
                    s[last_value] = m
                    map[mixuid] = s
            elif last_name != "":
                for key in map:
                    s = map[key]
                    for v in s:
                        print key+"\t"+v+"\t"+str(s[v])
                if len(map) == 0:
                    print last_name+"\t"+last_value+"\t"+last_count
                map.clear()
            last_name = mixuid
            last_time = time
            last_value = value
            last_count = count
        except Exception, e:
            print traceback.format_exc()