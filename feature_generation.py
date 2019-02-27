
# Dataset Configurator
meta = {}

attr_list = {}
attr_list['id'] = "id"
attr_list['attr_str'] = ['link']
attr_list['attr_num'] = ['']
attr_list['target'] = 'target'

meta['attr_list'] = attr_list
meta['values'] = {}


# Count distinct values
threshold = 10
for x in meta['attr_str']:
    count_values_df = pandas.DataFrame(df[x].value_counts(), columns=['name','cnt'])
    count_values_df = count_values_df[count_values_df.cnt >= threshold]
    other_count = count_values_df['cnt'][count_values_df.cnt < threshold].sum()
    count_values_df.append(("OTHER", other_count))
    meta['values'] = count_values_df

for string_attr in meta['attr_list']['attr_str']:
    df.groupby(meta['id']).agg({string_attr:{string_attr+"_mode":lambda x: x.count_values().index[0]}})
    for attr_value in meta['values'][string_attr]:
        df.groupby(meta['id']).agg({attr_value:["first","last","count","nunique",""] })