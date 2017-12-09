#coding=utf-8
import sys, os
exist_mix_uid = ''
if __name__ == '__main__':
    #lines = ['a\t4031\t1','a\t4032\t0.3','a\t4041\t0','b\t407\t1']
    #for line in lines:
    for line in sys.stdin:
        try:
            mix_uid, tags, score = line.strip().split('\t')
            if mix_uid != exist_mix_uid:
                print mix_uid + '\t' + tags + '\t' + score
            elif type_id != tags[0:3]:
                print mix_uid + '\t' + tags + '\t' + score

            exist_mix_uid = mix_uid
            type_id = tags[0:3]
        except Exception, e:
            #print e
            continue
    try:
        if mix_uid != exist_mix_uid:
            print mix_uid + '\t' + tags + '\t' + score
        elif type_id != tags[0:3]:
            print mix_uid + '\t' + tags + '\t' + score
    except Exception, e:
        pass

