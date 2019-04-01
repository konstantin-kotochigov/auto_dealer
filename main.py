import sys
from pyspark.sql import SparkSession
from hdfs import InsecureClient

import time
import datetime

from cj_loader import CJ_Loader
from cj_predictor import CJ_Predictor
from cj_export import CJ_Export



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




def main():
    
    send_update = True if len(sys.argv) >= 2 and (sys.argv[1]=="1") else False
    print("Send_update = {}".format(send_update))
    update_model_every = 60*24*7 # in seconds
    
    start_processing = time.time()
    
    # Common classes
    spark = SparkSession.builder.appName('analytical_attributes').getOrCreate()
    wd = "/user/kkotochigov/"
    hdfs_client = InsecureClient("http://159.69.60.183:50070", "hdfs")
    
    
    # Check whether We Need to Refit
    model_modification_ts = next(iter([x[1]['modificationTime'] for x in hdfs_client.list(wd+"models/", status=True) if x[0] == "model.h5"]), None)
    model_needs_update = True if (model_modification_ts == None) or (time.time() - model_modification_ts > update_model_every) else False
    print("Refit = {}".format(model_needs_update))
    
    # Load Data
    cjp = CJ_Loader(spark)
    cjp.set_organization("57efd33d-aaa5-409d-89ce-ff29a86d78a5")
    cjp.load_cj(ts_from=(2010,1,1), ts_to=(2020,1,1))
    # cjp.cj_stats(ts_from=(2010,12,1), ts_to=(2020,12,31))
    cjp.cj_data.createOrReplaceTempView('cj')
    cjp.extract_attributes()
    cjp.process_attributes()
    data = cjp.cj_dataset
    
    # Make Model
    predictor = CJ_Predictor(wd+"models/")
    predictor.set_data(data)
    # predictor.optimize()
    
    print("Update Model = {}".format(model_needs_update))
    result = predictor.fit(update_model=model_needs_update)
    
    print("Got Result Table with Rows = {}".format(result.shape[0]))
    print("Score Distribution = \n{}".format(result.return_score.value_counts(sort=False)))
    
    
    
    # Make Delta
    df = spark.createDataFrame(result)
    dm = CJ_Export("57efd33d-aaa5-409d-89ce-ff29a86d78a5", "model_update", "http://159.69.60.183:50070", "schema.avsc")
    
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
    
    # Publish Delta
    print("Send Update To Production = {}".format(send_update))
    dm.make_delta(df, mapping, send_update=send_update)
    
    finish_processing = time.time()
    
    # Store Run Metadata
    log_data = [datetime.datetime.today().strftime('%Y-%m-%d %H-%m'),
        str(cjp.cj_data_rows),
        str(cjp.cj_df_rows),
        str(cjp.cj_dataset_rows),
        str(model_needs_update),
        str(send_update),
        str(round((finish_processing - start_processing)/60, 2)),
        str(predictor.train_auc),
        str(predictor.test_auc),
        str(predictor.test_auc_std)
    ]
    log = ";".join(log_data)
    
    log_path = wd+"log/log.csv"
    
    if "log.csv" not in hdfs_client.list(wd+"log/"):
        data_with_header = 'dt;loaded_rows;extracted_rows;processed_rows;refit;send_to_prod;processing_time;train_auc;test_auc;test_auc_std\n'+log + "\n"
        hdfs_client.write(log_path, data=bytes(data_with_header, encoding='utf8'), overwrite=True)
    else:
            with hdfs_client.read(log_path) as reader:
                prev_log = reader.read()
            new_log = prev_log + bytes(log + "\n", encoding='utf8')
            hdfs_client.write(log_path, data=new_log, overwrite=True)
        



main()







        
