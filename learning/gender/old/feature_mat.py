import numpy as np


d_user = {}
d_feature = {}
f_user = open('tags30_mixuid_gender.txt','r')
f_feature = open('dict_tags_selected.txt','r')

# nb_f_user = 0
# for line in f_user:
#     nb_f_user += 1

f_user2 = open('tags30_mixuid_gender.txt','r')
# nb_f_user = len(f_user2.readlines())
# print(nb_f_user)
max_tmp = 0
for line in f_user2:
    line = line.strip()
    # print(line)
    [mix_uid, index, gender] = line.split('\t')
    index = int(index)
    if index >= max_tmp:
        max_tmp = index

# max_tmp.astype(int)
y_train = np.zeros(max_tmp+1,dtype=np.int32)
# y_train.dtype='int16'
print("dtype",y_train.dtype)
print("y_train",y_train)
print("y_type",type(y_train))
for line in f_user:
    line = line.strip()
    # print(line)
    [mix_uid, index, gender] = line.split('\t')
    index = int(index)
    d_user[mix_uid] = index
    # print(gender)
    if gender == 'Male':
        y_train[index] =1
        # print(type(y_train[index]))
# f_user21 = open('y_train.txt','wb')
np.savetxt('y_train.txt',y_train,fmt='%d')


for line in f_feature:
    line = line.strip()
    [feature, index] = line.split('\t')
    index = int(index)
    d_feature[feature] = index


nb_rows = max_tmp
nb_cols = len(d_feature)
print(nb_cols)

feature_mat = np.zeros((nb_rows, nb_cols),dtype=np.int8)
data_sample = open('tags_30.txt','r')


for line in data_sample:
    [mix_uid, feature, gender] = line.split('\t')
    try:
        feature_mat[d_user[mix_uid]][d_feature[feature]] = 1
    except:
        pass



np.savetxt('X_train.txt',feature_mat,fmt='%d')