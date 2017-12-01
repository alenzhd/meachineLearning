#!/usr/bin/python
# -*- coding:utf-8 -*-

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from sklearn import svm
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score


# 'sepal length', 'sepal width', 'petal length', 'petal width'
iris_feature = u'花萼长度', u'花萼宽度', u'花瓣长度', u'花瓣宽度'


if __name__ == "__main__":
    path = '..\\8.Regression\\iris.data'  # 数据文件路径
    data = pd.read_csv(path, header=None) #pands 读取csv文件
    x, y = data[[0,1,2,3]], data[4] #x取前4列，y取最后一列
    y = pd.Categorical(y).codes #归属的类别数字化为0，1，2
    print("y",y)
    x = x[[0, 1]]   #x降成两维，预测的结果精度从0.98到0.8，为了方便等下画图，所以把它降维，
    #调用模型选择的数据划分方法，把数据划给训练集为60%，测试集为40%，
    #random_state是某个值后，重复调用这个函数，结果是固定的
    x_train, x_test, y_train, y_test = train_test_split(x, y, random_state=1, train_size=0.6)

    # 分类器，decision_function_shape=ovr代表是四个二分类器，one-vs-rest('ovr'),one-vs-one ('ovo')代表的是 n_classes * (n_classes - 1) / 2个分类器
    #svc kernel常用的有两种，一种是线性的，还有一种是rbf,高斯的，一般选择高斯
    # C是惩罚系数,过小，对超出ε管道的样本数据惩罚就小，训练误差变大；C过大，学习精度相应提高，但模型的泛化能力变差。
    # 另外，C的值影响到对样本中“离群点”（噪声影响下非正常数据点）的处理，选取合适的C就能在一定程度上抗干扰，从而保证模型的稳定性
    #gamma是你选择径向基函数作为kernel后，该函数自带的一个参数。隐含地决定了数据映射到新的特征空间后的分布。
    # clf = svm.SVC(C=0.1, kernel='linear', decision_function_shape='ovr')
    clf = svm.SVC(C=1, kernel='rbf', gamma=5, decision_function_shape='ovr')#初始化模型
    clf.fit(x_train, y_train.ravel())#训练模型 ravel()将多维数组降位一维返回的是视图 numpy.flatten()返回的是一份copy
    print("x_test",x_test)
    print('预测值',clf.predict(x_test)) #clf.predice(x_test)代表输出预测的的值，是个ndarray
    # 准确率
    print (clf.score(x_train, y_train))  # clf.score(x_train,y_train)代表是准确率，是个数字
    print ('训练集准确率：', accuracy_score(y_train, clf.predict(x_train))) #等价于clf.score()
    print (clf.score(x_test, y_test))
    print ('测试集准确率：', accuracy_score(y_test, clf.predict(x_test)))
    print ('decision_function:\n', clf.decision_function(x_test)) #输出每行数据到3个类型支持向量平面的距离，预测是选取平面距离最大的平面
    # decision_function 到超平面线性支持向量的距离
    print ('decision_function:\n', clf.decision_function(x_train))
    print ('\npredict:\n', clf.predict(x_train))

    # 画图
    print("x",x)
    x1_min, x2_min = x.min()
    print("x1_min",x1_min)
    print("x2_min",x2_min)
    x1_max, x2_max = x.max()
    print("x1_max",x1_max)
    print("x2_max",x2_max)
    x1, x2 = np.mgrid[x1_min:x1_max:500j, x2_min:x2_max:500j]  # 生成网格采样点,选择500是因为这样分线会平滑很多
    grid_test = np.stack((x1.flat, x2.flat), axis=1)  # 测试点 flat类似flatten(),降成1维
    print ('grid_test = \n', grid_test)
    Z = clf.decision_function(grid_test)    # 样本到决策面的距离
    print (Z)
    grid_hat = clf.predict(grid_test)       # 预测分类值
    grid_hat = grid_hat.reshape(x1.shape)  # 使之与输入的形状相同
    mpl.rcParams['font.sans-serif'] = [u'SimHei'] #设置字体格式
    mpl.rcParams['axes.unicode_minus'] = False

    cm_light = mpl.colors.ListedColormap(['#A0FFA0', '#FFA0A0', '#A0A0FF']) #设置颜色
    cm_dark = mpl.colors.ListedColormap(['g', 'r', 'b'])
    plt.figure(facecolor='w')
    plt.pcolormesh(x1, x2, grid_hat, cmap=cm_light)
    plt.scatter(x[0], x[1], c=y, edgecolors='k', s=50, cmap=cm_dark)      # 样本
    plt.scatter(x_test[0], x_test[1], s=120, facecolors='none', zorder=10)     # 圈中测试集样本
    plt.xlabel(iris_feature[0], fontsize=13)
    plt.ylabel(iris_feature[1], fontsize=13)
    plt.xlim(x1_min, x1_max)
    plt.ylim(x2_min, x2_max)
    plt.title(u'鸢尾花SVM二特征分类', fontsize=16)
    plt.grid(b=True, ls=':')
    plt.tight_layout(pad=1.5)
    plt.show()
