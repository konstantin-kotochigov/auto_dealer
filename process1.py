# Script to Load Customer Journey Data

# Output: train_df, score_df, cj, cp

import datetime
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import SparkSession

import pandas
import numpy
from sklearn.model_selection import train_test_split, StratifiedShuffleSplit
from sklearn.metrics import roc_auc_score

import keras
import keras.preprocessing
from keras.layers import Concatenate
from keras.layers import MaxPooling1D
from keras.layers import Embedding, LSTM, Dense, PReLU, Dropout, BatchNormalization
from keras.layers import Conv1D
from keras.preprocessing import sequence
from keras.layers import Input, Dense, Reshape, Flatten, Dropout, multiply, GaussianNoise
from keras.layers import BatchNormalization, Activation, Embedding, ZeroPadding2D
from keras.models import Sequential, Model
import keras.backend as K

class CJ_Loader:
    
    cj_path = ""
    cp_path = ""
    
    cj_data = None
    cj_attributes = None
    cj_dataset = None
    cj_df = None
    
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
    
    def load_cj(self, ts_from, ts_to):
        cj_all = self.spark.read.format("com.databricks.spark.avro").load(self.cj_path)
        time_from = int(time.mktime(datetime.datetime(ts_from[0],ts_from[1],ts_from[2]).timetuple())) * 1000
        time_to = int(time.mktime(datetime.datetime(ts_to[0], ts_to[1], ts_to[2]).timetuple())) * 1000
        self.cj_data = cj_all.filter('ts > {} and ts < {}'.format(time_from, time_to))
        
    def cj_stats(self):
        cj_all = self.spark.read.format("com.databricks.spark.avro").load(self.cj_path)
        cj_all.selectExpr("from_unixtime(min(ts/1000)) as max_ts","from_unixtime(max(ts/1000)) as min_ts","count(*) as cnt").show()
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
        cj_df_attrs = self.spark.sql("select fpc, tpc, link, ts, lead(ts) over (partition by fpc order by ts) as next_ts from cj_df")
        cj_df_attrs = cj_df_attrs.withColumn("next",(cj_df_attrs["next_ts"]-cj_df_attrs["ts"]) / 3600)
        cj_df_attrs.createOrReplaceTempView("cj_df_attrs")
        self.cj_df = cj_df_attrs.select("fpc","tpc","ts","next","link")
        
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
    
    
    def export_datamart(self, output_file):
        
        self.cj_dataset.to_parquet(output_file, index=False)
        

class Predictor():
    
    input_data = None
    return_model = None
    
    def set_data(self, input_data):
        self.input_data = input_data
    
    def preprocess_data(self):
        data = self.input_data
        data['dt'] = data.dt.apply(lambda x: list(x)[0:len(x)-1])
        data.loc[:, 'dt'] = data.dt.apply(lambda r: [0]*(32-len(r)) + r if pandas.notna(numpy.array(r).any()) else [0]*32 )
        data.dt = data.dt.apply(lambda r: numpy.log(numpy.array(r)+1))
        Max = numpy.max(data.dt.apply(lambda r: numpy.max(r)))
        data.dt = data.dt.apply(lambda r: r / Max)
        data.dt = data.dt.apply(lambda r: r if len(r)==32 else r[-32:])
        tk = keras.preprocessing.text.Tokenizer(filters='', split=' ')
        tk.fit_on_texts(data.url.values)
        urls = tk.texts_to_sequences(data.url)
        urls = sequence.pad_sequences(urls, maxlen=32)
        dt = numpy.concatenate(data.dt.values).reshape((len(data), 32, 1))
        return (urls, dt, data['target'], tk)
    
    def create_network(self, tk):
        # architecure for Neuron Network
        max_len = 32
        SEQUENCE_LENGTH = 128
        EMBEDDING_DIM = 64
        input_1 = Input(shape=(32,))
        model_1 = Embedding(len(tk.word_index)+1, EMBEDDING_DIM,
                           input_length=max_len, trainable=True)(input_1)
        model_1 = (Conv1D(64, (3), activation='relu', padding='same'))(model_1)
        model_1 = MaxPooling1D((2), padding='same')(model_1)
        model_1 = (Conv1D(64, (3), activation='relu', padding='same'))(model_1)
        model_1 = MaxPooling1D((2), padding='same')(model_1)
        model_1 = (Conv1D(128, (3), activation='relu', padding='same'))(model_1)
        model_1 = MaxPooling1D((2), padding='same')(model_1)
        model_1 = Reshape((128,4))(model_1)
        model_1 = LSTM(units = SEQUENCE_LENGTH, dropout_W=0.2, dropout_U=0.2,
                         return_sequences=True )(model_1)
        model_1 = LSTM(32, dropout_W=0.2, dropout_U=0.2)(model_1)
        input_2 = Input(shape=(32,1))
        model_2 = LSTM(units = SEQUENCE_LENGTH, dropout_W=0.2, dropout_U=0.2,
                         return_sequences=True )(input_2)
        model_2 = LSTM(32, dropout_W=0.2, dropout_U=0.2)(model_2)
        model_3 = Concatenate(axis=-1)([model_1, model_2])
        model_3 = Dense(32)(model_3)
        model_3 = PReLU()(model_3)
        model_3 = Dropout(0.2)(model_3)
        model_3 = BatchNormalization()(model_3)
        model_3 = Dense(16)(model_3)
        model_3 = PReLU()(model_3)
        model_3 = Dropout(0.2)(model_3)
        model_3 = BatchNormalization()(model_3)
        model_3 = Dense(8)(model_3)
        model_3 = PReLU()(model_3)
        model_3 = Dropout(0.2)(model_3)
        model_3 = BatchNormalization()(model_3)
        model_3 = Dense(1)(model_3)
        model_3 = Activation('sigmoid')(model_3)
        return_model = Model(inputs=[input_1, input_2], outputs=model_3)
        def mean_pred(y_true, y_pred):
            return K.mean(y_pred)
        return_model.compile(loss='binary_crossentropy',
                    optimizer='Adam',
                    metrics=['accuracy'])
        self.return_model = return_model
        return return_model
    
    # def main():
    #     import load
    #    import functions
    #    import cj_lstm
    #    import lstm
    #    import export
    #    return -1
    
    # data = pandas.read_parquet("/home/kkotochigov/bmw_cj_lstm.parquet")
    # data1 = pandas.read_parquet("/home/kkotochigov/bmw_cj_lstm1.parquet")
    
    def optimize(self):
        
        auc = []
        urls, dt, y, tk = self.preprocess_data()
        model = self.create_network(tk)
        
        cv_number = 0
        for cv_train_index, cv_test_index in StratifiedShuffleSplit(n_splits=1, test_size=0.25).split(y,y):
            print("train length = {}, test length = {}".format(len(cv_train_index), len(cv_test_index)))
            cv_number += 1
            print("CV number = {}".format(cv_number))
            train = ([urls[cv_train_index], dt[cv_train_index]], y[cv_train_index])
            test = ([urls[cv_test_index], dt[cv_test_index]], y[cv_test_index])
            model.fit(train[0], train[1], epochs=1, batch_size=1024, shuffle = True)
            current_auc = roc_auc_score(test[1], model.predict(test[0]))
            print(current_auc)
            auc.append(current_auc)
        
        print("average AUC = {}, std AUC = {}".format(numpy.mean(auc), numpy.std(auc)))
        
    
    def fit(self):
        
        urls, dt, y, tk = self.preprocess_data()
        model = self.create_network(tk)
        
        train_data = ([urls, dt], y)
        scoring_data = [urls[data.target==0], dt[data.target==0]]
        model.fit(train_data[0], train_data[1], epochs=1, batch_size=1024, shuffle = True)
        pred = model.predict(scoring_data)
        self.result = pandas.DataFrame({"fpc":data.fpc[data.target==0], "tpc":data.tpc[data.target==0], "return_score":pred.reshape(-1)})
        
        self.result['return_score'] = pandas.cut(result.return_score, 5, labels=['1','2','3','4','5'])
        
        return result
        
    
    
    
import time
import json
from hdfs import InsecureClient
from uuid import uuid4

class DeltaMaker:
    def __init__(self, dmp_organization_id, source_name, hdfs_host, cp_schema_path, hdfs_user='hdfs'):
        self.cid = dmp_organization_id
        self.source = source_name
        self.host = hdfs_host
        self.user = hdfs_user
        self.raw_schema = open(cp_schema_path).read()  # TODO: omg add some exceptions
    def make_delta(self, df, mapping, send_update=True):
        timestamp = int(time.time() * 1e3)
        #  very bad. TODO: need to pass than into foreachPartition some other way
        self.timestamp = timestamp
        self.mapping = mapping
        df.rdd.foreachPartition(self.process_partition)
        hdfs_client = InsecureClient(self.host, user=self.user)
        hdfs_client.write('/data/{}/.dmpkit/profiles/{}/cdm/ts={}/_SUCCESS'.format(self.cid, self.source, timestamp),
                          data="")
        hdfs_client.set_owner('/data/{}/.dmpkit/profiles/{}/cdm/ts={}'.format(self.cid, self.source, timestamp),
                              owner='dmpkit')
        if send_update:
            update_id = str(uuid4())
            update_path = '/data/{}/.dmpkit/profiles/.updates/{}.json'.format(self.cid, update_id)
            update_content = {"id": update_id, "owner": self.cid, "dataset": "profiles", "source": self.source,
                              "created": timestamp,
                              "path": '/data/{}/.dmpkit/profiles/{}/cdm/ts={}'.format(self.cid, self.source, timestamp),
                              "mergeStrategy": "merge"}
            hdfs_client.write(update_path, data=json.dumps(update_content), encoding='utf-8')
            hdfs_client.set_owner(update_path, owner='dmpkit')
    def process_partition(self, partition):
        import json
        import time
        from uuid import uuid4
        from hdfs import InsecureClient
        from avro.datafile import DataFileWriter
        from avro.io import DatumWriter
        from avro.schema import Parse
        import os
        now = int(time.time() * 1e3)
        hdfs_client = InsecureClient(self.host, user=self.user)
        json_list = []
        for row in partition:
            profile_version = 393320  # TODO: remove magic constant, validate it's value
            system = [{'id': {'primary': 10000, 'secondary': 10000}, 'value': self.cid, 'confidence': 1.0},
                      {'id': {'primary': 10000, 'secondary': 10001}, 'confidence': 1.0, 'value': now}
                      ]
            id = []
            for k in self.mapping['id']:
                if k in row and row[k] is not None:
                    id.append({'id': self.mapping['id'][k], 'confidence': 1.0, 'value': str(row[k])})
            attributes = []
            for k in self.mapping['attributes']:
                if k in row and row[k] is not None:
                    if 'mapping' in self.mapping['attributes'][k]:
                        if row[k] in self.mapping['attributes'][k]['mapping']:
                            attributes.append({'id': {'secondary': self.mapping['attributes'][k]['mapping'][row[k]],
                                                      'primary': self.mapping['attributes'][k]['primary']},
                                               'confidence': 1.0})
                    else:
                        attributes.append(
                            {'id': {'secondary': -1, 'primary': self.mapping['attributes'][k]['primary']},
                             'confidence': 1.0,
                             'value': row[k]})
            json_dict = {
                'id': id,
                'attributes': attributes,
                'system': system
            }
            json_list.append(json_dict)
        filename = 'part_{}.avro'.format(str(uuid4()))
        clever_schema = Parse(self.raw_schema)
        with DataFileWriter(open(filename, "wb"), DatumWriter(), clever_schema) as writer:
            for record in json_list:
                writer.append(record)
        hdfs_client.upload(
            '/data/{}/.dmpkit/profiles/{}/cdm/ts={}/{}'.format(self.cid, self.source, self.timestamp, filename),
            filename)
        os.remove(filename)








mapping = {
    'id': {
        'fpc': {
            'primary': 10008,
            'secondary': 10031
        },
        'tpc': {
            'primary':10005,
            'secondary':-1
        }
    },
    'attributes': {
        'return_score': {
            'primary': 10127,
            'mapping': {
                '1': 10000,
                '2': 10001,
                '3': 10002,
                '4': 10003,
                '5': 10004
            }
        }
    }
}

def main():

    dryrun = arg[1]

    spark = SparkSession.builder.appName('analytical_attributes').getOrCreate()
    cjp = CJ_Loader(spark)
    cjp.set_organization("57efd33d-aaa5-409d-89ce-ff29a86d78a5")
    cjp.load_cj(ts_from=(2018,12,1), ts_to=(2018,12,31))
    cjp.cj_data.createOrReplaceTempView('cj')
    cjp.extract_attributes()
    cjp.process_attributes()
    data = cjp.cj_dataset
    
    predictor = Predictor()
    predictor.set_data(data)
    # predictor.optimize()
    result = predictor.fit()
    
    if dryrun==False:
        
        df = spark.createDataFrame(result)
        dm = DeltaMaker("57efd33d-aaa5-409d-89ce-ff29a86d78a5", "model_update", "http://159.69.60.183:50070", "schema.avsc")
        dm.make_delta(df, mapping, send_update=True)
        
    stats = spark.read.csv("")
    new_stats = spark.createDataFrame([[cjp.cj_data.count(),dryrun]])
    stats = stats.union(new_stats)
    new_stats.write.csv("")
        
if __name__ == "__main__":
    main()







