#!/usr/bin/python
# -*- coding:utf-8 -*-
import numpy as np
import math
import matplotlib.pyplot as plt
import matplotlib as mpl
def gauss(num):
    origin = 0
    sigma = 1
    x = np.linspace(origin-num*sigma,origin+num*sigma,100)
    y = np.exp(-(x-origin)**2/2*sigma**2)/((num*sigma)*math.sqrt(2*math.pi))
    # print ('x = \n', x)
    # print (y.shape)
    # print ('y = \n', y)
      #绘制正态分布概率密度函数
    mpl.rcParams['font.sans-serif'] = [u'SimHei']  #FangSong/黑体 FangSong/KaiTi
    mpl.rcParams['axes.unicode_minus'] = False
    plt.figure(facecolor='w')
    plt.plot(x,y,'r-',x,y,'go',linewidth=2,markersize=8)
    plt.xlabel('X',fontsize=15)
    plt.ylabel('Y',fontsize=15)
    plt.title(u'高斯分布函数',fontsize=18)
    plt.grid(True)
    plt.show()
    # f(x) = {1 \over \sigma\sqrt{2\pi} }\,e^{- {{(x-\mu )^2 \over 2\sigma^2}}}
def calRatio():
    print(np.sqrt(6*np.sum(1/np.arange(1,10000,dtype=np.float)**2)))
def taylor():
    t = np.linspace(-2*np.pi)
if __name__ == "__main__":
    # gauss(5)
    calRatio()