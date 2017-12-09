# encoding=utf-8
import string,sys,os
import traceback
# words = pseg.cut("我爱北京天安门")
reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == '__main__':
    import jieba
    import jieba.posseg as pseg
    jieba.load_userdict("userdict.txt")
    reader = open("stopword.data")
    s = set()
    cx = ["wp"]
    for line in reader:
        s.add(line.strip())
    for line in sys.stdin:
        try:
            temp = line.strip().split("\t")
            if len(temp) < 3:continue
            keywords = temp[1]
            result = []
            result.append(temp[0])
            result.append(temp[1])
            words = pseg.cut(temp[1])
            ar = []
            for word, flag in words:
                if word not in s and flag not in cx:
                    ar.append(word)
            result.append(string.join(ar, " "))
            result.append(temp[2])
            print string.join(result, "\t")
        except Exception, e:
            print traceback.format_exc()
            continue
