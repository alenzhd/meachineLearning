#!/usr/bin/python
# -*- coding:utf-8 -*-

import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import Lasso, Ridge
from sklearn.model_selection import GridSearchCV


if __name__ == "__main__":
    # pandas读入
    data = pd.read_csv('Advertising.csv')    # TV、Radio、Newspaper、Sales
    x = data[['TV', 'Radio', 'Newspaper']]
    # x = data[['TV', 'Radio']]
    y = data['Sales']
    print (x)
    print (y)

    x_train, x_test, y_train, y_test = train_test_split(x, y, random_state=1)
    # model = Lasso()
    # model = Ridge()#岭回归
    from sklearn.linear_model import LogisticRegression
    model = LogisticRegression()
    alpha_can = np.logspace(-3, 2, 10) #等比数列
    np.set_printoptions(suppress=True)#设置数组可以显示，显示多位小数位
    print ('alpha_can = ', alpha_can)
    #GridSearchCV，它存在的意义就是自动调参，只要把参数输进去，就能给出最优化的结果和参数。但是这个方法适合于小数据集，一旦数据的量级上去了，很难得出结果。这个时候就是需要动脑筋了。
    # 数据量比较大的时候可以使用一个快速调优的方法——坐标下降。它其实是一种贪心算法：拿当前对模型影响最大的参数调优，直到最优化；再拿下一个影响最大的参数调优，如此下去，直到所有的参数调整完毕。
    # 这个方法的缺点就是可能会调到局部最优而不是全局最优，但是省时间省力，巨大的优势面前，还是试一试吧，后续可以再拿bagging再优化。
    lasso_model = GridSearchCV(model, param_grid={'alpha': alpha_can}, cv=5)
    lasso_model.fit(x_train, y_train)
    print ('超参数：\n', lasso_model.best_params_)
    #argsort 从中可以看出argsort函数返回的是数组值从小到大的索引值
    order = y_test.argsort(axis=0)
    y_test = y_test.values[order]
    x_test = x_test.values[order, :]
    y_hat = lasso_model.predict(x_test)
    print( lasso_model.score(x_test, y_test))
    mse = np.average((y_hat - np.array(y_test)) ** 2)  # Mean Squared Error
    rmse = np.sqrt(mse)  # Root Mean Squared Error
    print (mse, rmse)

    t = np.arange(len(x_test))
    mpl.rcParams['font.sans-serif'] = [u'simHei']
    mpl.rcParams['axes.unicode_minus'] = False
    plt.figure(facecolor='w')
    plt.plot(t, y_test, 'r-', linewidth=2, label=u'真实数据')
    plt.plot(t, y_hat, 'g-', linewidth=2, label=u'预测数据')
    plt.title(u'线性回归预测销量', fontsize=18)
    plt.legend(loc='upper right')
    plt.grid()
    plt.show()
