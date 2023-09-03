import json
import os
import pandas as pd

# items = os.listdir("osr_single_node")
# print(len(items))
# for items in os.walk("osr_single_node"):
#     print('...............................................')
#     print(items)
name_list = []
useful_data = pd.DataFrame(columns=['name', 'instance_type', 'timestamp', 'elapsed_time'])

i = 0
j = 0
for main_dir, sub_dir_list, sub_file_list in os.walk("osr_single_node"):
    # if i == 2:
    #     break
    print(i,  '........................................')


    raw_name = main_dir[16:]
    instance_type = raw_name.split("_")[0]
    # name_list.append(raw_name)

    #print(type(main_dir), sub_file_list)
    if len(sub_file_list) > 0:
        file_name = main_dir + r'/'+sub_file_list[0]
        #print(file_name)
        with open(file_name, 'r') as f:
            data = json.load(f)
            #print(data)
            if data['completed'] == True:
                useful_data.loc[j] = [raw_name, instance_type, data['timestamp'], data['elapsed_time']]
                j += 1

    i += 1

print('finally!!!!!')
print(useful_data)
useful_data.to_csv('single_node', index=False)

