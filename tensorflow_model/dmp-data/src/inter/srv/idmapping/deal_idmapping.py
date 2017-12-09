#!/usr/bin/env python
# -*-coding:utf-8 -*-

import sys

dict_flag_type = {'imei':'IM','idfa':'IA','mac':'MA','cuid':'CU'}

if __name__ == '__main__':
    exist_mix_uid , res_str = '', ''
    mix_uid, flag_id, flag, model = '','','',''
    # 标准输入
    #lines = ['ad1\tc\t132\tad','ad2\tc\t132\tad']
    #for line in lines:
    for line in sys.stdin:
        try:
            line = line.strip()
            mix_uid, flag_id, flag, model= line.split('\t')
            if model == 'uid2id': #输出UUMP
                if exist_mix_uid != '' and mix_uid != exist_mix_uid :
                    print 'UUMP_'+ exist_mix_uid + '\t' + res_str[:-1] + '\t' + 'UUMP'
                    res_str = flag + ':' + flag_id + ','
                else:
                    res_str = res_str + flag + ':' + flag_id + ','
            else: #输出XXUU 
                print dict_flag_type[flag]+'UU_'+flag_id+'\t'+mix_uid+'\t'+dict_flag_type[flag]+'UU'

            exist_mix_uid = mix_uid

        except Exception, e:
            pass

    try:
        if model == 'uid2id': #输出UUMP
            print 'UUMP_'+ exist_mix_uid + '\t' + res_str[:-1] + '\t' + 'UUMP'
        else: #输出XXUU
            print dict_flag_type[flag]+'UU_'+flag_id+'\t'+mix_uid+'\t'+dict_flag_type[flag]+'UU'
    except Exception,e:
        pass
