import numpy as np
import matplotlib.pyplot as plt
from sklearn import datasets
from sklearn.linear_model import LogisticRegression 
from sklearn import preprocessing
import time
from sklearn.cross_validation import cross_val_score
from sklearn import svm

# # ============== 读数据 ============================
# print('loading data...')
# mat_origin = np.loadtxt('X_train.txt')
# print('finish loading...')

# # ============== 归一化 ============================
# min_max_scaler = preprocessing.MinMaxScaler()
# X = min_max_scaler.fit_transform(mat_origin)
# print('特征矩阵大小', X.shape)

# y = np.loadtxt('y_train.txt')

# # 分类器选逻辑回归
# clf = LogisticRegression()

# # ============== Cross Validation =================
# metric = cross_val_score(clf, X, y,cv=5,scoring='accuracy')
# print(metric)



print('load feature matrix...')
start_load = time.clock()
mat_origin = np.loadtxt('X_train.txt')
end_load = time.clock()
print('load time spent',end_load - start_load)

# 矩阵归一化
min_max_scaler = preprocessing.MinMaxScaler()
mat = min_max_scaler.fit_transform(mat_origin)
# mat = mat_origin
print('load feature matrix... Done!')
y_mat = np.loadtxt('y_train.txt')


present_seuil = 5

print('当一个uv至少有%s条有效特征时才做出有效分类'%(present_seuil))
nb_l,nb_c = mat.shape
useful_sample = 0
for i in range(nb_l):
    tmp = sum(mat[i,:])
    if tmp >= present_seuil:
        useful_sample += 1


mat_useful = np.zeros((useful_sample,nb_c))
y_useful = np.zeros(useful_sample)
index = 0

mean_lst = []
useful_lst = []
nb_0 = 0
for i in range(nb_l):
    tmp = sum(mat[i,:])
    if tmp == 0:
        nb_0 += 1

    mean_lst.append(tmp)
    # print(tmp)
    if tmp >= present_seuil:
        mat_useful[index,:] = mat[i,:]
        useful_lst.append(tmp)
        y_useful[index] = y_mat[i]
        index += 1

print('平均tags数量',np.mean(mean_lst))
print('useful平均tags数量',np.mean(useful_lst))
print('有效样本数量',index,useful_sample)


X = mat_useful
y = y_useful

# nb_l,nb_c = X.shape

# print('1类的数量',sum(y))
# print('0类的数量',len(y)-sum(y))
# nb_0 = len(y)-sum(y)
# new_X = np.zeros((3 * nb_0,nb_c))
# new_y = np.zeros(3 * nb_0)
# tmp = 0
# for i in range(nb_l):
#     if y[i] == 0:
#         tmp = np.zeros((1,nb_c))
#         for index in range(len(X[i,:])):
#             tmp[0,index] = X[i,index]

#         for j in range(3):
#             X = np.concatenate((X,tmp))
#             y = np.concatenate((y,np.zeros(0)))
            # print('tmp_now',tmp)
            # tmp += 1

# now_X = np.concatenate((X,new_X))
# new_y = np.concatenate((y,new_y))

# X = new_X
# y = new_y

# 分类器选逻辑回归
clf = LogisticRegression(penalty='l1',random_state=2,class_weight='balanced')
# clf = LogisticRegression(class_weight='balanced')
# clf = svm.SVC(class_weight='balanced')

# y_test = clf.predict(X_test)

# ============== Cross Validation =================
metric = cross_val_score(clf, X, y,cv=5,scoring='accuracy')
print(metric)
clf.fit(mat,y_mat)
coef_lst = clf.coef_

f_coef = open('model_coeffient.txt','w')

for i in coef_lst.T:
    i = i.strip('[')
    i = i.strip(']')
    f_coef.write(i+'\n')