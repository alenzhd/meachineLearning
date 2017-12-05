import numpy as np
import pandas as pd
d_user = {}
d_feature = {}
path = 'X_train.txt' # 数据文件路径
data = pd.read_csv(path, header=None,)
# print("data",data)
print(data.shape)
print(data[[0][0]][0])
# print(data[[0]][0].shape)
# x = data[[]]
# f_user2 = open('X_train.txt','r')
# # nb_f_user = len(f_user2.readlines())
# # print(nb_f_user)
# max_tmp = 0
# for line in f_user2:
#     line = line.strip()
#     # print(line)
#     print(line)
