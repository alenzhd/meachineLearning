import numpy as np
import pandas as pd

d_user = {}
d_feature = {}
path = 'X_train.txt' # 数据文件路径
data = pd.read_csv(path, header=None,)
# print("data",data)
print(data.shape)
# print(data)
# print(data[[0]])
x = data[[0]]
y = pd.Categorical(data[1]).codes
print('y',y)
print(y.shape)
print(y[0],y[3]) #male 1,female 0

feature_mat = np.zeros((29740, 6313))

print(feature_mat.shape)
# print(x[[0]])
print(x.shape)
print(x[0][0])
print(len(x[0][0].split(',')))
# for line in x:
#     print(line,"")
# f_feature = open('dict_tags_selected.txt','r')
# for line in f_feature:
#     line = line.strip()
#     [feature, index] = line.split('\t')
#     index = int(index)
#     d_feature[feature] = index
# print(d_feature)
# nb_cols = len(d_feature)
# data_sample = open('data_train_flow.txt','r')
# for line in data_sample:
#     [mix_uid, feature, gender] = line.split('\t')
#     try:
#         feature_mat[d_user[mix_uid]][d_feature[feature]] = 1
#     except:
#         pass
#
#
#
# np.savetxt('X_train.txt',feature_mat)