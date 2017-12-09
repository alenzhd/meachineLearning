#!/usr/bin/env python
# -*-coding:utf-8 -*-

import sys
from itertools import starmap
from operator import mul

dict_res = {'1':'82957','2':'82958','3':'82959','4':'82960','5':'82961'}

if __name__ == '__main__':

    f = open('age_feature_model.txt','r') # 新的模型参数保留成两列，选择的特征与对应的权重
    index_value = 0
    dict_age = {} # 这个保存特征位置的字典
    dict_weight = {}
    clf = [] # 这个保存模型权重的list
    last_feature = ''
    for line in f:
        line = line.strip()
        # print line
        [feature, age, weight] = line.split('\t')
        dict_weight[feature + '_' + age] = weight
    feature_vector = [0.0] * 6
    last_mix_uid , last_user_type= None, None
    cnt , max_score, max_age = 0, 0, 0
    # 标准输入读人群
    #lines = ['a\tc\t133\t1\tad','a\tc\t132\t1\tad','a\tc\t130\t1\tad','a\tc\t135\t1\tad','a\tc\t136\t1\tad', 'b\tc\t137\t1\tad']
    #lines = ['a\tc\t133\t2.0\tad','a\tc\t134\t2.0\tad','a\tc\t1\t1\tad']
    #for line in lines:
    for line in sys.stdin:
        try:
            line = line.strip()
            mix_uid, type_id, tags, weight, user_type = line.split('\t')
            if last_mix_uid != None and (mix_uid != last_mix_uid or user_type != last_user_type):
                if cnt >= 3:               #三个标签以上才会输出
                    for i in range(1, 6):
                        if feature_vector[i] > max_score:
                            max_score = feature_vector[i]   #最大权重
                            max_age = i                     #最大权重所属年龄段
                        #print feature_vector[i]
                    #输出结果   
                    print (last_mix_uid + '\t' + dict_res[str(max_age)] + '\t' + str(max_score) + '\t' + last_user_type)
                    cnt, max_score = 0, 0                  #初始化
                    feature_vector = [0.0] * 6             #初始化
            elif type_id + '_' + tags + '_1' in dict_weight:  #判断特征模型中是否有此标签
                cnt = cnt + 1              #计标签数
                for i in range(1, 6):
                    feature_vector[i] = float(feature_vector[i]) + float(weight) * float(dict_weight[type_id + '_' + tags + '_' + str(i)])
                    #print feature_vector[i]
                #print '____________':



            '''
                for i in range(1, 6):
                    #print type_id + '_' + tags + '_' + str(i)
                    if type_id + '_' + tags + '_' + str(i) in dict_weight:  #判断特征模型中是否有此标签
                        feature_vector[i] = float(feature_vector[i]) + float(weight) * float(dict_weight[type_id + '_' + tags + '_' + str(i)])
            '''
            last_mix_uid = mix_uid
            last_user_type = user_type

        except Exception as e:
            #print e
            pass

    try:
        if cnt >= 3:               #三个标签以上才会输出
            for i in range(1, 6):
                if feature_vector[i] > max_score:
                    max_score = feature_vector[i]
                    max_age = i
                    #print feature_vector[i]
            print (last_mix_uid + '\t' + dict_res[str(max_age)] + '\t' + str(max_score) + '\t' + last_user_type)
    except Exception as e:
        #print e
        pass

