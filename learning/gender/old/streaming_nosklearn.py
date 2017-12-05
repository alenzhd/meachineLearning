#!/usr/bin/env python
# -*-coding:utf-8 -*-

import sys
from itertools import starmap
from operator import mul


if __name__ == '__main__':
    
    # 读特征字典
    # f_dict_tags = open('dict_tags_selected.txt','r')
    # dict_tags = {}
    # for line in f_dict_tags:
    #     [key, index] = line.split('\t')
    #     index = int(index)
    #     max_index = index
    #     dict_tags[key] = index
    # dim_feature = max_index + 1

    # # 读训练好的模型
    # clf = []
    # for line in open('model_coeffient.txt','r'):
    #     line = line.strip()
    #     clf.append(float(line))

    f = open('model_coeffient_new.txt','r') # 新的模型参数保留成两列，选择的特征与对应的权重
    index_value = 0
    dict_tags = {} # 这个保存特征位置的字典
    clf = [] # 这个保存模型权重的list
    for line in f:
        line = line.strip()
        [feature, weight] = line.split('\t')
        dict_tags[feature] = index_value
        index_value += 1
        clf.append(float(weight))

    dim_feature = index_value + 1




    feature_vector = [0] * dim_feature
    last_mix_uid = None

    # 标准输入读人群
    for line in sys.stdin:
        try:
            line = line.strip()
            [mix_uid, actionType, actionIndex] = line.split('\t')
            
            if last_mix_uid != None and mix_uid != last_mix_uid:
                if sum(feature_vector) >= 3:
                    # class_pred = clf.predict(feature_vector)
                    prob = sum(starmap(mul, zip(feature_vector, clf))) 
                    class_pred = 'Unknown'
                    # print('prob', prob)
                    if prob > 0: # Logistique Regression 判决
                        class_pred = 'Male'
                    elif prob < 0:
                        class_pred = 'Female'
                    print("\t".join([last_mix_uid, class_pred]))
                else:
                    pass

                feature_vector = [0] * dim_feature

            key_flow = actionType + '_' + actionIndex

            if key_flow in dict_tags:
                feature_vector[dict_tags[key_flow]] = 1
            else:
                pass

            last_mix_uid = mix_uid

        except:
            pass

    try:
        if sum(feature_vector) >= 0:
            prob = sum(starmap(mul, zip(feature_vector, clf))) 
            class_pred = 'Unknown'
            if prob > 0: # Logistique Regression 判决
                class_pred = 'Male'
            elif prob < 0:
                class_pred = 'Female'
            print("\t".join([last_mix_uid, class_pred]))
    except :
        pass
