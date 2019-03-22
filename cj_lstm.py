# Run functions.py!

output_file = "/home/kkotochigov/bmw_cj_lstm.parquet"

def get_link(raw_link):
    return (substring(substring_index(substring_index(raw_link, '?', 1), '#', 1), 19, 100))

# Get attributes from CJ data
cj_df = spark.sql('''
select
    cj_id(id, 10008, 10031)[0] as fpc,
    id.gid as tpc,
    substring(substring_index(substring_index(cj_attr(attributes, 10071)[0], '?', 1), '#', 1), 19, 100) as link,
    ts/1000 as ts
from cj c
''').filter("tpc is not null and link is not null and fpc is not null")

# Compute TS deltas between events (in hours)
cj_df.createOrReplaceTempView("cj_df")
cj_df_attrs = spark.sql("select fpc, tpc, link, ts, lead(ts) over (partition by fpc order by ts) as next_ts from cj_df")
cj_df_attrs = cj_df_attrs.withColumn("next",(cj_df_attrs["next_ts"]-cj_df_attrs["ts"]) / 3600)
cj_df_attrs.createOrReplaceTempView("cj_df_attrs")

# Assign surrogate ID to FPCs (as an optional optimization)
# fpc_dict = spark.sql("select fpc, rank() over (order by fpc) as rnk from (select distinct fpc from cj_df)")
# fpc_dict.createOrReplaceTempView("fpc_dict")
# cj_df = spark.sql("select t1.link, t1.next, t2.rnk as id, t1.ts from cj_df_attrs t1 join fpc_dict t2 on t1.fpc = t2.fpc order by t1.ts").select("id","ts","next","link")
cj_df = cj_df_attrs.select("fpc","tpc","ts","next","link")

# Create Records
def groupAttrs(r):
    sortedList = sorted(r[1], key=lambda y: y[0])
    dividedList = list(zip(*sortedList))
    # dt = [x[0] for x in sortedList]
    deltas = [i for i, x in enumerate(dividedList[1]) if x == None or x > 4]
    return [(r[0], dividedList[3][y], dividedList[1][0:y+1], dividedList[2][0:y+1], 0 if dividedList[1][y]==None else 1) for y in deltas]

# test = spark.sql("select fpc, ts, tpc, rank() over (partition by fpc order by tpc) as rnk from cj_df")

# Import data in LSTM format
y = cj_df.select(['fpc','tpc','ts','next','link']).rdd.map(lambda x: (x['fpc'], (x['ts'], x['next'], x['link'], x['tpc']))).groupByKey().flatMap(lambda x: groupAttrs(x))
y_py = pandas.DataFrame(y.collect(),  columns=['fpc','tpc','dt','url','target'])

# Convert lists to Text
y_py['url'] = y_py.url.apply(lambda x:" ".join(x))
y_py['dt'] = y_py.dt.apply(lambda x:[y for y in x if y != None])

# ToDO: convert Nones in dt and export to parquet instead of  CSV
y_py.to_parquet(output_file, index=False)


