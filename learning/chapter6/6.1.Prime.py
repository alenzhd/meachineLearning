#!/usr/bin/python
# -*- coding:utf-8 -*-

import operator
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
from time import time
import math


def is_prime(x):
    return 0 not in [x % i for i in range(2, int(math.sqrt(x)) + 1)]
def return_prime():
   p = [p for p in range(a, b) if 0 not in [p % d for d in range(2, int(math.sqrt(p)) + 1)]]
   return  p

def is_prime3(x):
    flag = True
    for p in p_list2:
        if p > math.sqrt(x):
            break
        if x % p == 0:
            flag = False
            break
    if flag:
        p_list2.append(x)
    return flag



if __name__ == "__main__":
    a = 2
    b = 100000

    # 方法1：直接计算
    # t = time()
    # # p = [p for p in range(a, b) if 0 not in [p % d for d in range(2, int(math.sqrt(p)) + 1)]]
    # p = [p for p in range(a, b) if 0 not in [p % d for d in range(2, int(math.sqrt(p)) + 1)]]
    # print (time() - t)
    # print (p)
    #
    # # 方法2：利用filter
    # t = time()
    # p = filter(is_prime, range(a, b))
    # p_list = [item for item in p]
    # print (time() - t)
    #
    # print(type(p))
    # print ("p_list",p_list)
    # #
    # # # 方法3：利用filter和lambda
    # t = time()
    # is_prime2 = (lambda x: 0 not in [x % i for i in range(2, int(math.sqrt(x)) + 1)])
    # p = filter(is_prime2, range(a, b))
    # p_list = [item for item in p]
    # print (time() - t)
    # print ("p_list",p_list)
    # #
    # # # 方法4：定义
    # t = time()
    # p_list = []
    # for i in range(2, b):
    #     flag = True
    #     for p in p_list:
    #         if p > math.sqrt(i):
    #             break
    #         if i % p == 0:
    #             flag = False
    #             break
    #     if flag:
    #         p_list.append(i)
    # print (time() - t)
    # print (p_list)
    #
    # # 方法5：定义和filter
    p_list2 = []
    # t = time()
    # filter(is_prime3, range(2, b))
    # print (time() - t)
    # print (p_list2)

    print ('---------------------')
    # a = 1180
    # b = 1230
    a = 1600
    b = 1700
    p_list2 = []
    s = filter(is_prime3, range(2, b+1))
    c = [item for item in s]
    p = np.array(c)
    # p = np.array(np.arange(10))
    # p = [item for item in p.tolist()]
    print(type(p))
    p = p[p >= a]
    print (p)
    print(len(p))
    p_rate = float(len(p)) / float(int(b)-a+1)
    print ('素数的概率：', p_rate, '\t',)
    print ('公正赔率：', 1/p_rate)
    print ('合数的概率：', 1-p_rate, '\t',)
    print ('公正赔率：', 1 / (1-p_rate))
