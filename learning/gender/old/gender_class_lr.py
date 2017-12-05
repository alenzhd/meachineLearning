import numpy as np
import matplotlib.pyplot as plt
from sklearn import datasets
from sklearn.linear_model import LogisticRegression 
from sklearn import preprocessing
import time
from sklearn.cross_validation import cross_val_score
# from sklearn.svm import LinearSVC
from sklearn import svm
from sklearn.externals import joblib




print('load feature matrix...')
start_load = time.clock()
mat_origin = np.loadtxt('X_train.txt')
print(mat_origin.shape)
print(mat_origin[0][0])
# end_load = time.clock()
# print('load time spent',end_load - start_load)
#
# # 矩阵归一化
# min_max_scaler = preprocessing.MinMaxScaler()
# mat = min_max_scaler.fit_transform(mat_origin)
# # mat = mat_origin
# print('load feature matrix... Done!')
# y_mat = np.loadtxt('y_train.txt')
#
#
# present_seuil = 5
#
# nb_l,nb_c = mat.shape
# useful_sample = 0
# for i in range(nb_l):
#     tmp = sum(mat[i,:])
#     if tmp >= present_seuil:
#         useful_sample += 1
#
#
# mat_useful = np.zeros((useful_sample,nb_c))
# y_useful = np.zeros(useful_sample)
# index = 0
# for i in range(nb_l):
#     tmp = sum(mat[i,:])
#     if tmp >= present_seuil:
#         mat_useful[index,:] = mat[i,:]
#         y_useful[index] = y_mat[i]
#         index += 1
#
# print('有效样本数量',index,useful_sample)
#
#
# y_mat = y_useful
# mat = mat_useful
# nb_train = int(index * 0.8)
# nb_test = nb_l - nb_train
# X_train = mat[:nb_train,:]
#
#
# y_train = y_mat[:nb_train]
#
# X_test = mat[nb_train:,:]
# y_test = y_mat[nb_train:]
# # X_test = mat[1000:1200]
# classifier = LogisticRegression(class_weight='balanced')
# # classifier = svm.SVC(class_weight='balanced')
#
#
#
# print('training...')
# start_train = time.clock()
# classifier.fit(X_train, y_train)
#
#
# joblib.dump(classifier, 'lr.model')
# lr = joblib.load('lr.model')
#
# print('predicting...')
# # class_pred = classifier.predict(X_test)
# class_pred = lr.predict(X_test)
# a = class_pred
# print('归于1类百分比',sum(a)/len(a))
#
#
# length_test = len(class_pred)
# correct_classify = 0
# for i in range(length_test):
#     if class_pred[i] == y_test[i]:
#         correct_classify += 1
#
# print(class_pred)
# print(sum(class_pred))
# print(len(class_pred))
# print('accuracy', correct_classify/length_test)
#
#
# coef_lst = lr.coef_
#
# # f_coef = open('model_coeffient.txt','w')
#
# # for i in coef_lst.T:
# #     i = str(i).strip('[')
# #     i = str(i).strip(']')
# #     f_coef.write(i+'\n')
#
# f_coef = open('model_coeffient_new.txt','w')
#
# nb_coef = len(coef_lst)
#
# f_tags = open('dict_tags_selected.txt','r')
# coef_lst = coef_lst.T
#
# nb = 0
# for line in f_tags:
#     line = line.strip()
#     [feature, index] = line.split('\t')
#     coef = str(coef_lst[nb])
#     coef = coef.strip('[')
#     coef = coef.strip(']')
#     res = feature + '\t' + coef+ '\n'
#     f_coef.write(res)
#     nb = nb + 1
#
# f_coef.close()
# f_tags.close()