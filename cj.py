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
        for attr in cj_attributes:
            for member_id in range(0, 8):
                member_name = 'member' + str(member_id)
                if attr is not None and member_name in attr:
                    if attr[member_name] is not None and 'id' in attr[member_name]:
                        if attr[member_name]['id'] == arg_id and ('key' not in attr[member_name] or attr[member_name]['key'] == arg_key):
                            result += [attr[member_name]['value']]
    return result

spark.udf.register("cj_attr", cj_attr, ArrayType(StringType()))

def cj_get_ids(cj_ids):
    result = []
    for id in cj_ids['uids']:
        result.append(str(id['id']) + "_" + str(id['key']))
    return result

spark.udf.register("cj_get_ids1", cj_get_ids, ArrayType(StringType()))

cj_df = spark.sql('''
select
    cj_get_ids1(id) as crmid,
    date(from_unixtime(ts/1000)) as ts
from cj c
''')

cj_df.groupby("crmid").count().show()






cj_df = spark.sql('''
select
    cj_id(id, 10008, 10031)[0] as fpc,
    cj_id(id, 10008, 10032)[0] as userid,
    date(from_unixtime(ts/1000)) as ts
from cj c
''').filter("fpc is null and userid is null")

cj_df.createOrReplaceTempView("cj_df")
cj_df = spark.sql("select fpc,  count(distinct ts) as cnt from cj_df group by fpc")
cj_df.createOrReplaceTempView("cj_df")

userid_cnt = cj_df.select("userid").distinct().count()
event_stat = cj_df.groupby("userid").count().toDF("userid","cnt").groupby("cnt").count().orderBy("cnt")


cj_features_raw = spark.sql('''select cj_id(id, 10008, 10032)[0] as crmid, date(from_unixtime(ts/1000)) as ts from cj''').filter("crmid is not null and crmid!='undefined'").groupby("crmid").count()
# cj_features_raw = spark.sql("select cj_id(id, 10008, 10032) as ts from cj")
# spark_df = spark.createDataFrame(df)_

cj_features_raw_py = cj_features_raw.collect()
cj_ids = pandas.DataFrame([(float(x[0]),x[1]) for x in cj_features_raw_py], columns=['crmid','ts'])

available_train_py = train_py[train_py.app_ts >= datetime.date(2018,12, 25)][["crmid","app_ts"]]
available_scoring_py = train_py[train_py.app_ts >= datetime.date(2018,12, 25)][["crmid","app_ts"]]


cj_df = cj_ids.join(df[['crmid','app_ts']].set_index("crmid"), on="crmid", how='inner').groupby("crmid", as_index=False).count()[["crmid","ts"]]
# cj_df_scoring = cj_ids.join(scoring_py[['crmid','app_ts']].set_index("crmid"), on="crmid", how='inner').groupby("crmid", as_index=False).count()[["crmid","ts"]]
df = df.join(cj_df.set_index("crmid"), on="crmid", how="left")
# scoring_py = scoring_py.join(cj_df_scoring.set_index("crmid"), on="crmid", how="left")
df["ts"] = df.ts.fillna(0)
# scoring_py["ts"] = scoring_py.ts.fillna(0)

numeric_attrs.append("ts")

# cj_df.createOrReplaceTempView("cj_df")

base.fillna(app_ts => current_dt)
base = base.filter(app_ts > ts and ts > app_ts - 2W)

features = spark.sql("""
    select
        crmid,
        app_ts,
        sum(case when device_type==10000 then 1 else 0 end) as f1_cnt,
        sum(case when device_type==10001 then 1 else 0 end) as f2_cnt,
        sum(case when device_type==10002 then 1 else 0 end) as f3_cnt,
        sum(case when device_type==10003 then 1 else 0 end) as f4_cnt,
        max(case when device_type==10000 then ts else to_date('1900-01-01') end) as f1_prev,
        max(case when device_type==10001 then ts else to_date('1900-01-01') end) as f2_prev,
        max(case when device_type==10002 then ts else to_date('1900-01-01') end) as f3_prev,
        max(case when device_type==10003 then ts else to_date('1900-01-01') end) as f4_prev,
        count(*) as cnt
    from
        cj_df
    group by
        crmid,
        app_ts
""")

features.coalesce(1).write.parquet("")