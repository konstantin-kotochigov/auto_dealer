import pandas
import time
import datetime

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import SparkSession

class CJ_Loader:
    
    cj_path = ""
    cp_path = ""
    
    cj_data = None
    cj_attributes = None
    cj_dataset = None
    cj_df = None
    
    cj_data_rows = None
    cj_df_rows = None
    cj_dataset_rows = None
    
    def __init__(self, spark):
        self.cj_path = ""
        self.cp_path = ""
        self.spark = spark
        self.spark.udf.register("cj_id", self.cj_id, ArrayType(StringType()))
        self.spark.udf.register("cj_attr", self.cj_attr, ArrayType(StringType()))
        
    def set_organization(self, org_uid="57efd33d-aaa5-409d-89ce-ff29a86d78a5"):
        self.cj_path = "/data/{}/.dmpkit/customer-journey/master/cdm".format(org_uid)
        self.cp_path = "/data/{}/.dmpkit/profiles/master/cdm".format(org_uid)
    
    def load_cj_all(self):
        self.cj_data = self.spark.read.format("com.databricks.spark.avro").load(self.cj_path)
        self.cj_data_rows = self.cj_data.count()
        print("Loaded CJ Rows = {}".format(self.cj_data_rows))
    
    def load_cj(self, ts_from, ts_to):
        cj_all = self.spark.read.format("com.databricks.spark.avro").load(self.cj_path)
        time_from = int(time.mktime(datetime.datetime(ts_from[0],ts_from[1],ts_from[2]).timetuple())) * 1000
        time_to = int(time.mktime(datetime.datetime(ts_to[0], ts_to[1], ts_to[2]).timetuple())) * 1000
        self.cj_data = cj_all.filter('ts > {} and ts < {}'.format(time_from, time_to))
        self.cj_data_rows = self.cj_data.count()
        print("Loaded CJ Rows = {}".format(self.cj_data_rows))
        
    def cj_stats(self, ts_from=(2000,1,1), ts_to=(2100,1,1)):
        cj_all = self.spark.read.format("com.databricks.spark.avro").load(self.cj_path)
        time_from = int(time.mktime(datetime.datetime(ts_from[0],ts_from[1],ts_from[2]).timetuple())) * 1000
        time_to = int(time.mktime(datetime.datetime(ts_to[0], ts_to[1], ts_to[2]).timetuple())) * 1000
        cj_all = cj_all.filter('ts > {} and ts < {}'.format(time_from, time_to))
        cj_all.selectExpr("date(from_unixtime(ts/1000)) as ts").groupBy("ts").count().orderBy("ts").show(100)
        cj_all.selectExpr("date(from_unixtime(min(ts/1000))) as max_ts","date(from_unixtime(max(ts/1000))) as min_ts","count(*) as cnt").show()
        return -1
    
    @staticmethod
    def cj_id(cj_ids, arg_id, arg_key=-1):
        result = []
        for id in cj_ids['uids']:
            if id['id'] == arg_id and id['key'] == arg_key:
                result += [id['value']]
        return result
    
    @staticmethod
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
        
    
    # Method to get attributes (returns dataframe)
    def extract_attributes(self):
        
        # Link processing function
        def __get_link(raw_link):
            return (substring(substring_index(substring_index(raw_link, '?', 1), '#', 1), 19, 100))
        
        # Select CJ Attributes
        cj_df = self.spark.sql('''
            select
                cj_id(id, 10008, 10031)[0] as fpc,
                id.gid as tpc,
                substring(substring_index(substring_index(cj_attr(attributes, 10071)[0], '?', 1), '#', 1), 19, 100) as link,
                ts/1000 as ts
            from cj c
        ''').filter("tpc is not null and link is not null and fpc is not null")
        
        # Compute TS deltas between events (in hours)
        cj_df.createOrReplaceTempView("cj_df")
        cj_df_attrs = self.spark.sql("select fpc, tpc, link, ts, lead(ts) over (partition by fpc order by ts) as next_ts from cj_df")
        cj_df_attrs = cj_df_attrs.withColumn("next",(cj_df_attrs["next_ts"]-cj_df_attrs["ts"]) / 3600)
        cj_df_attrs.createOrReplaceTempView("cj_df_attrs")
        self.cj_df = cj_df_attrs.select("fpc","tpc","ts","next","link")
        
        self.cj_df_rows = self.cj_df.count()
        print("Extracted Rows (cj_df) = {}".format(self.cj_df_rows))
        
        return
    
    
    def process_attributes(self):
    
        # Create Records
        def groupAttrs(r):
            sortedList = sorted(r[1], key=lambda y: y[0])
            dividedList = list(zip(*sortedList))
            # dt = [x[0] for x in sortedList]
            deltas = [i for i, x in enumerate(dividedList[1]) if x == None or x > 4]
            return [(r[0], dividedList[3][y], dividedList[1][0:y+1], dividedList[2][0:y+1], 0 if dividedList[1][y]==None else 1) for y in deltas]
        
        # Slice By User
        y = self.cj_df.select(['fpc','tpc','ts','next','link']).rdd.map(lambda x: (x['fpc'], (x['ts'], x['next'], x['link'], x['tpc']))).groupByKey().flatMap(lambda x: groupAttrs(x))
        
        # Convert To Pandas dataframe
        y_py = pandas.DataFrame(y.collect(),  columns=['fpc','tpc','dt','url','target'])
        
        # Process Types
        y_py['url'] = y_py.url.apply(lambda x:" ".join(x))
        y_py['dt'] = y_py.dt.apply(lambda x:[y for y in x if y != None])
        
        self.cj_dataset = y_py
        
        self.cj_dataset_rows = self.cj_dataset.shape[0]
        print("Dataset of Processed Rows (cj_dataset) = {}".format(self.cj_dataset_rows))
        
        return