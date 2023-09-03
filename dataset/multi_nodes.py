import json
import os
import pandas as pd

# items = os.listdir("osr_single_node")
# print(len(items))
# for items in os.walk("osr_single_node"):
#     print('...............................................')
#     print(items)
name_list = []
useful_data = pd.DataFrame(columns=['name', 'instance_type', 'number', 'timestamp', 'elapsed_time', 'datasize',
                                    'framework', 'input_size', 'program', 'workload'])

i = 0
j = 0
for main_dir, sub_dir_list, sub_file_list in os.walk("osr_multiple_nodes"):
    # if i == 2:
    #     break
    print(i,  '........................................')


    raw_name = main_dir[19:]
    instance_type = ''
    number = -1
    if raw_name:
        instance_type = raw_name.split("_")[1]
        number = int(raw_name.split("_")[0])
        #print(raw_name, instance_type, number)
    # name_list.append(raw_name)
    #print(main_dir, sub_file_list)
    if len(sub_file_list) > 0 and number > 0:
        #file_name = main_dir + r'/'+sub_file_list[0]
        file_name = main_dir + r'/report.json'

        #print(file_name)
        with open(file_name, 'r') as f:
            try:
                data = json.load(f)
                if data['completed'] == True:
                    useful_data.loc[j] = [raw_name, instance_type, number, data['timestamp'], data['elapsed_time'],
                                          data['datasize'], data['framework'], data['input_size'], data['program'],
                                          data['workload']]
                    j += 1
            except Exception as e:
                print(e)
                print('the file name is ', file_name)

    i += 1

print('finally!!!!!')
print(useful_data)
useful_data.to_csv('multiple_nodes_new', index=False)

