def get_link(raw_link):
    return (substring(substring_index(substring_index(raw_link, '?', 1), '#', 1), 19, 100))

# Get attributes from CJ data
cj_df1 = spark.sql('''
select
    cj_id(id, 10008, 10031)[0] as fpc,
    substring(substring_index(substring_index(cj_attr(attributes, 10071)[0], '?', 1), '#', 1), 19, 100) as link,
    ts/1000 as ts
from cj c
''').filter("fpc is not null")

# Compute TS deltas between events (in hours)
cj_df.createOrReplaceTempView("cj_df")
cj_df_attrs = spark.sql("select fpc, link, device, ts, lead(ts) over (partition by fpc order by ts) as next_ts from cj_df")
cj_df_attrs = cj_df_attrs.withColumn("next",(cj_df_attrs["next_ts"]-cj_df_attrs["ts"]) / 3600)
cj_df_attrs.createOrReplaceTempView("cj_df_attrs")

# Assign surrogate ID to FPCs
fpc_dict = spark.sql("select fpc, rank() over (order by fpc) as rnk from (select distinct fpc from cj_df)")
fpc_dict.createOrReplaceTempView("fpc_dict")
cj_df = spark.sql("select t1.link, t1.device, t1.next, t2.rnk as id, t1.ts from cj_df_attrs t1 join fpc_dict t2 on t1.fpc = t2.fpc order by t1.ts").select("id","ts","device","next","link")

# Create Records
def groupAttrs(r):
    sortedList = sorted(r[1], key=lambda y: y[0])
    dividedList = list(zip(*sortedList))
    # dt = [x[0] for x in sortedList]
    deltas = [i for i, x in enumerate(dividedList[1]) if x == None or x > 4]
    return [(r[0], dividedList[1][0:y+1], dividedList[2][0:y+1], 0 if dividedList[1][y]==None else 1) for y in deltas]

# Import data in LSTM format
y = cj_df.select(['id','ts','next','link']).rdd.map(lambda x: (x['id'], (x['ts'], x['next'], x['link']))).groupByKey().flatMap(lambda x: groupAttrs(x))
y_py = pandas.DataFrame(y.collect(),  columns=['id','dt','url','target'])
y_py.to_csv("/home/kkotochigov/bmw_cj_data.csv", index=False)







y_py = pandas.read_csv("/home/kkotochigov/bmw_cj_data.csv")




