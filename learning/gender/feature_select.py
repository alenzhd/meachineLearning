import sys

file_name_male = 'male_tags_desc.txt'
file_name_female = 'female_tags_desc.txt'

##########
f_male = open(file_name_male,'r')
d_male = {}
for line in f_male:
    line = line.strip()
    [gender, action_type, nb, score] = line.split('\t')
    key = action_type + '_' + nb
    d_male[key] = float(score)

#############
f_female = open(file_name_female,'r')
d_female = {}
for line in f_female:
    line = line.strip()
    [gender, action_type, nb, score] = line.split('\t')
    key = action_type + '_' + nb
    d_female[key] = float(score)

# seuil_present = sys.argv[1] # 0.01 for example
# seuil_diff = sys.argv[2] # 0.1 for example
seuil_present = 0.001
seuil_diff = 0.3
seuil_present = float(seuil_present)
seuil_diff = float(seuil_diff)


f_feature_diff = open('f_feature_diff.txt','w')
res_lst = []
for key in d_male:
    if key in d_female:
        if d_male[key] <= seuil_present or d_female[key] <= seuil_present:
            continue
        # diff = abs((d_male[key] - d_female[key])/min(d_male[key],d_female[key]))
        diff = abs((d_male[key] - d_female[key])/min(d_male[key],d_female[key]))
        
        if diff > seuil_diff:
            res_lst.append([key,diff])
            f_feature_diff.write('%s\t%s\n'%(key,diff))
        # res_lst.append([key,diff])
        # f_feature_diff.write('%s\t%s\n'%(key,diff))
# print(res_lst)
#
d_tags_selected = {}
index = 0

file_d_tags_selected = open('dict_tags_selected.txt','w')
for ele in res_lst:
    # print(ele)
    # print(ele[0])
    d_tags_key = ele[0]
    d_tags_value = index
    d_tags_selected[d_tags_key] = d_tags_value
    index += 1
    file_d_tags_selected.write('%s\t%s\n'%(d_tags_key,d_tags_value))

print(d_tags_selected)
print('特征维度',len(res_lst))
#
#
#
nb_male = 0
# if __name__ == '__main__':
#     f_mix = open('mixuid_gender_all.txt','r')
#     f_out_dict = open('dict_mixuid.txt','w')
#     dict_mix_gender = {}
#     index = 0
#     for line in f_mix:
#         line = line.strip()
#         [mix,gender] = line.split('\t')
#         if mix not in dict_mix_gender:
#             # if nb_male <= 10000:
#             f_out_dict.write('%s\t%s\t%s\n'%(mix,index,gender))
#                 # if gender == 'Male':
#                     # nb_male += 1
#         dict_mix_gender[mix] = index
#         index += 1

# clf = joblib.load('lr.model')

