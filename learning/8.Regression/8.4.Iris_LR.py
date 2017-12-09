#!/usr/bin/python
# -*- coding:utf-8 -*-

import numpy as np
import pandas as pd
from sklearn import preprocessing
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.pipeline import Pipeline
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
from sklearn import preprocessing
from sklearn.linear_model import LogisticRegression,LogisticRegressionCV
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.pipeline import Pipeline
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.patches as mpatches
from sklearn.model_selection import train_test_split,cross_val_score
from sklearn.metrics import accuracy_score
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import classification_report
from sklearn.metrics import roc_curve,auc

if __name__ == "__main__":

    # # # 手写读取数据
    # f = file(path)
    # x = []
    # y = []
    # for d in f:
    #     d = d.strip()
    #     if d:
    #         d = d.split(',')
    #         y.append(d[-1])
    #         x.append(map(float, d[:-1]))
    # print '原始数据X：\n', x
    # print '原始数据Y：\n', y
    # x = np.array(x)
    # print 'Numpy格式X：\n', x
    # y = np.array(y)
    # print 'Numpy格式Y - 1:\n', y
    # y[y == 'Iris-setosa'] = 0
    # y[y == 'Iris-versicolor'] = 1
    # y[y == 'Iris-virginica'] = 2
    # print 'Numpy格式Y - 2:\n', y
    # y = y.astype(dtype=np.int)
    # print 'Numpy格式Y - 3:\n', y
    # print '\n\n============================================\n\n'

    # # 使用sklearn的数据预处理
    # df = pd.read_csv(path, header=None)
    # x = df.values[:, :-1]
    # y = df.values[:, -1]
    # print x.shape
    # print y.shape
    # print 'x = \n', x
    # print 'y = \n', y
    # le = preprocessing.LabelEncoder()
    # le.fit(['Iris-setosa', 'Iris-versicolor', 'Iris-virginica'])
    # print le.classes_
    # y = le.transform(y)
    # print 'Last Version, y = \n', y

    # def iris_type(s):
    #     it = {'Iris-setosa': 0,
    #           'Iris-versicolor': 1,
    #           'Iris-virginica': 2}
    #     return it[s]
    #
    # # 路径，浮点型数据，逗号分隔，第4列使用函数iris_type单独处理
    # data = np.loadtxt(path, dtype=float, delimiter=',',
    #                   converters={4: iris_type})
    # path = 'iris.data'  # 数据文件路径
    path = 'test.data'
    # data = pd.read_csv(path, header=None)
    data = np.loadtxt(path)
    x, y = np.split(data, (4, ), axis=1) #以2为边界把数据分给x和y
    # data[3] = pd.Categorical(data[3]).codes
    print('data shape',data.shape)
    print(data)

    # x = data[[0,1,2]]

    # np.savetxt('test.data',x,fmt='%0.2f')
    # f_feature = open('test.data','r+')

    # y = data[[4]]
    # # x = np.asarray(x)
    # # y = np.asarray(y)
    print('x',x)
    y=y.ravel()
    print('y',y)
    # iris_types = data[4].unique()
    # print iris_types
    # for i, type in enumerate(iris_types):
    #     data.set_value(data[4] == type, 4, i)
    # x, y = np.split(data.values, (4,), axis=1)
    # y = np.array(y)
    x_train, x_test, y_train, y_test = train_test_split(x, y, train_size=0.8, random_state=1)
    # print('y_type',type(y))
    from sklearn.externals import joblib
    model = LogisticRegression()
    model.fit(x_train,y_train)
    # joblib.dump(model, "train_model.m")
    weight = model.coef_

    np.savetxt('weight.txt',weight.T,fmt='%0.10f')
    f = open('weight.txt')
    for line in f:
        print(line)
    # print(s)
    # # print(y.ravel())
    # print(x.shape,y.shape)
    print ('\t训练集准确率: %.2f%%' % (100 * accuracy_score(y_train,model.predict(x_train))))
    print ('\t测试集准确率: %.2f%%' % (100 * accuracy_score(y_test,model.predict(x_test))))
    # precisions = cross_val_score(model, x_train, y_train, cv=5, scoring='precision')
    # print (u'精确率：', np.mean(precisions), precisions)
    # recalls = cross_val_score(model, x_train, y_train, cv=5, scoring='recall')
    # print (u'召回率：', np.mean(recalls), recalls)
    # plt.scatter(recalls, precisions)
  # auc
  #   predictions=model.predict_proba(x_test)#每一类的概率
  #   false_positive_rate, recall, thresholds = roc_curve(y_test, predictions[:, 1])
  #   roc_auc=auc(false_positive_rate,recall)
  #   plt.title('Receiver Operating Characteristic')
  #   plt.plot(false_positive_rate, recall, 'b', label='AUC = %0.2f' % roc_auc)
  #   plt.legend(loc='lower right')
  #   plt.plot([0,1],[0,1],'r--')
  #   plt.xlim([0.0,1.0])
  #   plt.ylim([0.0,1.0])
  #   plt.ylabel('Recall')
  #   plt.xlabel('Fall-out')
  #   plt.show()
    # print 'x = \n', x
    # print 'y = \n', y
    # 仅使用前两列特征
    # x = x[:, :2]
    # lr = Pipeline([('sc', StandardScaler()),
    #                ('poly', PolynomialFeatures(degree=2)),
    #                ('clf', LogisticRegression()) ])
    # from sklearn.linear_model import LogisticRegressionCV
    # lr =  LogisticRegressionCV(Cs=np.logspace(-3, 4, 8), cv=5, fit_intercept=False)
    # from sklearn.model_selection import GridSearchCV
    # # lr = Pipeline([
    # #     ('poly', PolynomialFeatures(degree=2, include_bias=True)),
    # #     ('lr', LogisticRegressionCV(Cs=np.logspace(-3, 4, 8), cv=5, fit_intercept=False))
    # # ])
    # from sklearn.svm import SVC
    # alpha = np.logspace(-2, 2, 20)
    # models = LogisticRegressionCV(Cs=alpha, penalty='l2', cv=3)
    # # models = Pipeline([
    # #     ('LogisticRegression', LogisticRegressionCV(Cs=alpha, penalty='l2', cv=3)),
    # #     ('SVM(RBF)', (SVC(kernel='rbf', decision_function_shape='ovr'), param_grid={'C': np.logspace(-3, 4, 8), 'gamma': np.logspace(-3, 4, 8)}))])
    # # alpha_can = np.logspace(-3, 1, 10)
    # # lr = GridSearchCV(lr, param_grid={'alpha': alpha_can}, cv=10)
    # print(type(models))
    # models.fit(x, y.ravel())
    # # print ('最优参数：', lr.get_params.C_)
    # # print(models.best_params_)
    # y_hat = models.predict(x)
    # y_hat_prob = models.predict_proba(x)
    # np.set_printoptions(suppress=True)
    # print ('y_hat = \n', y_hat)
    # print ('y_hat_prob = \n', y_hat_prob)
    # print (u'准确度：%.2f%%' % (100*np.mean(y_hat == y.ravel())))
    # # 画图
    # N, M = 500, 500     # 横纵各采样多少个值
    # x1_min, x1_max = x[:, 0].min(), x[:, 0].max()   # 第0列的范围
    # x2_min, x2_max = x[:, 1].min(), x[:, 1].max()   # 第1列的范围
    # t1 = np.linspace(x1_min, x1_max, N)
    # t2 = np.linspace(x2_min, x2_max, M)
    # x1, x2 = np.meshgrid(t1, t2)                    # 生成网格采样点
    # x_test = np.stack((x1.flat, x2.flat), axis=1)   # 测试点
    #
    # # # 无意义，只是为了凑另外两个维度
    # # x3 = np.ones(x1.size) * np.average(x[:, 2])
    # # x4 = np.ones(x1.size) * np.average(x[:, 3])
    # # x_test = np.stack((x1.flat, x2.flat, x3, x4), axis=1)  # 测试点
    #
    # mpl.rcParams['font.sans-serif'] = [u'simHei']
    # mpl.rcParams['axes.unicode_minus'] = False
    # cm_light = mpl.colors.ListedColormap(['#77E0A0', '#FF8080', '#A0A0FF'])
    # cm_dark = mpl.colors.ListedColormap(['g', 'r', 'b'])
    # y_hat = lr.predict(x_test)                  # 预测值
    # y_hat = y_hat.reshape(x1.shape)                 # 使之与输入的形状相同
    # plt.figure(facecolor='w')
    # plt.pcolormesh(x1, x2, y_hat, cmap=cm_light)     # 预测值的显示
    # plt.scatter(x[:, 0], x[:, 1], c=y, edgecolors='k', s=50, cmap=cm_dark)    # 样本的显示
    # plt.xlabel(u'花萼长度', fontsize=14)
    # plt.ylabel(u'花萼宽度', fontsize=14)
    # plt.xlim(x1_min, x1_max)
    # plt.ylim(x2_min, x2_max)
    # plt.grid()
    # patchs = [mpatches.Patch(color='#77E0A0', label='Iris-setosa'),
    #           mpatches.Patch(color='#FF8080', label='Iris-versicolor'),
    #           mpatches.Patch(color='#A0A0FF', label='Iris-virginica')]
    # plt.legend(handles=patchs, fancybox=True, framealpha=0.8)
    # plt.title(u'鸢尾花Logistic回归分类效果 - 标准化', fontsize=17)
    # plt.show()
