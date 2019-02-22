# Script to Load Customer Journey Data

# Output: train_df, score_df, cj, cp

import datetime
import time
import pandas

from pyspark.sql.functions import lit, to_date

# Input parameters
cj_path = "/data/57efd33d-aaa5-409d-89ce-ff29a86d78a5/.dmpkit/customer-journey/master/cdm"
cp_path = "/data/57efd33d-aaa5-409d-89ce-ff29a86d78a5/.dmpkit/profiles/master/cdm"

# Load Customer Journey
cj_all = spark.read.format("com.databricks.spark.avro").load(cj_path)
time_from = int(time.mktime(datetime.datetime(2017, 12, 20).timetuple())) * 1000
time_to = int(time.mktime(datetime.datetime(2019, 12, 30).timetuple())) * 1000
cj = cj_all.filter('ts > {} and ts < {}'.format(time_from, time_to))
cj.createOrReplaceTempView('cj')

# Load Customer Profile
cp_all = spark.read.format("com.databricks.spark.avro").load(cp_path)
time_from = int(time.mktime(datetime.datetime(2017, 12, 20).timetuple())) * 1000
time_to = int(time.mktime(datetime.datetime(2019, 12, 30).timetuple())) * 1000
cp = cp_all.filter('ts > {} and ts < {}'.format(time_from, time_to))
cp.createOrReplaceTempView('cp')

cj_stats = spark.sql("select from_unixtime(ts/1000) as ts, count(*) as cnt from cj group by from_unixtime(ts/1000) order by ts")






