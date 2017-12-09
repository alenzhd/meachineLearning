#-*- coding:utf-8 -*-
import sys
flag_mapping ="123" \
              "123asdfa\n" \
              "dfadf"
print(flag_mapping)
print(type(flag_mapping))
print(flag_mapping.split("\n")[0])
# audience_id_split = audience_id.strip().split('_')
# if len(audience_id_split) == 4:
#     flag_tmp = audience_id_split[2]
#     flag = flag_mapping[flag_tmp.lower()]
#     print('获取用户ID类型',flag,1.1)
#     if(flag.__contains__("md532")):
#         flag= flag.split('_')[1]
#         print(flag)
# else:
#     print('无法获取用户ID类型')
#     # dropTmpTables()
#     sys.exit(1)