from pyspark.sql.types import ArrayType, StringType

def cj_id(cj_ids, arg_id, arg_key=-1):
    result = []
    for id in cj_ids['uids']:
        if id['id'] == arg_id and id['key'] == arg_key:
            result += [id['value']]
    return result

spark.udf.register("cj_id", cj_id, ArrayType(StringType()))

def cj_attr(cj_attributes, arg_id, arg_key=None):
    result = []
    if cj_attributes is not None:
        for attr in cj_attributes:1
            for member_id in range(0, 8):
                member_name = 'member' + str(member_id)
                if attr is not None and member_name in attr:
                    if attr[member_name] is not None and 'id' in attr[member_name]:
                        if attr[member_name]['id'] == arg_id and ('key' not in attr[member_name] or attr[member_name]['key'] == arg_key):
                            result += [attr[member_name]['value']]
    return result

spark.udf.register("cj_attr", cj_attr, ArrayType(StringType()))







# Get ID, link and TS
cj_df = spark.sql('''
select
    cj_id(id, 10008, 10031)[0] as fpc,
    substring(substring_index(substring_index(cj_attr(attributes, 10071)[0], '?', 1), '#', 1), 19, 100) as link,
    ts/1000 as ts
from cj c
''').filter("fpc is not null")

# Compute TS deltas between events (in hours)
cj_df.createOrReplaceTempView("cj_df")
cj_df_attrs = spark.sql("select fpc, link, ts, lead(ts) over (partition by fpc order by ts) as next_ts from cj_df")
cj_df_attrs = cj_df_attrs.withColumn("next",(cj_df_attrs["next_ts"]-cj_df_attrs["ts"]) / 3600)
cj_df_attrs.createOrReplaceTempView("cj_df_attrs")

# Assign surrogate ID to FPCs
fpc_dict = spark.sql("select fpc, rank() over (order by fpc) as rnk from (select distinct fpc from cj_df)")
fpc_dict.createOrReplaceTempView("fpc_dict")
cj_df = spark.sql("select t1.link, t1.next, t2.rnk as id, t1.ts from cj_df_attrs t1 join fpc_dict t2 on t1.fpc = t2.fpc order by t1.ts").select("id","ts","next","link")

# Create Records
def groupAttrs(r):
    sortedList = sorted(r[1], key=lambda y: y[0])
    dividedList = list(zip(*sortedList))
    # dt = [x[0] for x in sortedList]
    deltas = [i for i, x in enumerate(dividedList[1]) if x == None or x > 4]
    return [(r[0], dividedList[1][0:y+1], dividedList[2][0:y+1], 1 if dividedList[1][y]==None else 0) for y in deltas]

# Group all urls by ID
y = cj_df.rdd.map(lambda x: (x['id'], (x['ts'], x['next'], x['link']))).groupByKey().flatMap(lambda x: groupAttrs(x))

# Convert ot Python
y_py = pandas.DataFrame(y.collect(),  columns=['id','dt','url','target'])
y_py.head()

y_py.to_csv("/home/kkotochigov/bmw_cj_data.parquet", index=False)


cj_df['target'] = 1

def divide_into_sessions(r):
    return ([])



