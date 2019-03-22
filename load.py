# Script to Load Customer Journey Data

# Output: train_df, score_df, cj, cp

import datetime
import time

class cj_predictor:
    cj_path = ""
    cp_path = ""
    cj_data = None
    cj_attributes = None
    def __init__(self, spark):
        self.cj_path = ""
        self.cp_path = ""
        self.spark = spark
    def set_organization(self, org_uid="57efd33d-aaa5-409d-89ce-ff29a86d78a5"):
        self.cj_path = "/data/{}/.dmpkit/customer-journey/master/cdm".format(org_uid)
        self.cp_path = "/data/{}/.dmpkit/profiles/master/cdm".format(org_uid)
    def load_cj_all(self):
        self.cj_data = self.spark.read.format("com.databricks.spark.avro").load(self.cj_path)
    def load_cj(self, ts_from, ts_to):
        cj_all = self.spark.read.format("com.databricks.spark.avro").load(self.cj_path)
        time_from = int(time.mktime(datetime.datetime(ts_from[0],ts_from[1],ts_from[2]).timetuple())) * 1000
        time_to = int(time.mktime(datetime.datetime(ts_to[0], ts_to[1], ts_to[2]).timetuple())) * 1000
        self.cj_data = self.cj_all.filter('ts > {} and ts < {}'.format(time_from, time_to))


cjp = cj_predictor(spark)
cjp.set_organization("57efd33d-aaa5-409d-89ce-ff29a86d78a5")
cjp.load_cj(ts_from=(2018,12,1), ts_to=(2018,12,2))
cjp.cj_data.createOrReplaceTempView('cj')
cjp.extract_attributes()
cjp.export_datamart(intemediate_output_file)

