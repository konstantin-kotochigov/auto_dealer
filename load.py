# Script to Load Customer Journey Data

# Output: train_df, score_df, cj, cp

import datetime
import time

# Input parameters
cj_path = "/data/57efd33d-aaa5-409d-89ce-ff29a86d78a5/.dmpkit/customer-journey/master/cdm"
cp_path = "/data/57efd33d-aaa5-409d-89ce-ff29a86d78a5/.dmpkit/profiles/master/cdm"

# Load Customer Journey
cj_all = spark.read.format("com.databricks.spark.avro").load(cj_path)
time_from = int(time.mktime(datetime.datetime(2018, 12, 1).timetuple())) * 1000
time_to = int(time.mktime(datetime.datetime(2018, 12, 30).timetuple())) * 1000
cj = cj_all.filter('ts > {} and ts < {}'.format(time_from, time_to))
cj.createOrReplaceTempView('cj')

CJ()
cj.addParam("","")
cj.readData()
