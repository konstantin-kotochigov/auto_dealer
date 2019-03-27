import sys
from pyspark.sql import SparkSession
from hdfs import InsecureClient

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
    
    dryrun = sys.argv[1]
    send_update = False
    print("dryrun = {}".format(dryrun))
    
    spark = SparkSession.builder.appName('analytical_attributes').getOrCreate()
    wd = "/user/kkotochigov/"
    
    # Load Data
    cjp = CJ_Loader(spark)
    cjp.set_organization("57efd33d-aaa5-409d-89ce-ff29a86d78a5")
    cjp.load_cj(ts_from=(2018,12,1), ts_to=(2018,12,31))
    # cjp.cj_stats(ts_from=(2018,12,1), ts_to=(2018,12,31))
    cjp.cj_data.createOrReplaceTempView('cj')
    cjp.extract_attributes()
    cjp.process_attributes()
    data = cjp.cj_dataset
    
    # Make Model
    predictor = CJ_Predictor(wd+"models/")
    predictor.set_data(data)
    # predictor.optimize()
    
    # Check whether We Need to Refit
    hdfs_client = InsecureClient("http://159.69.60.183:50070", "hdfs")
    update_model = False
    available_updates = len([x for x in hdfs_client.list(predictor.model_path+"/updates") if x != "done"]) > 0
    first_run = "model.h5" not in hdfs_client.list(predictor.model_path)
    if (available_updates) | (first_run):
        update_model = True
    
    result = predictor.fit(update_model=update_model)
    
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
    if dryrun == "0":
        send_update = True
    dm.make_delta(df, mapping, send_update=send_update)
    
    log = spark.createDataFrame([[datetime.datetime.today().strftime('%Y-%m-%d %H-%m'),cjp.cj_data_rows, cjp.cj_df_rows,cjp.cj_dataset_rows]]).toDF("dt","loaded_rows","extracted_rows","processed_rows")
    log = "; ".join([datetime.datetime.today().strftime('%Y-%m-%d %H-%m'),str(cjp.cj_data_rows), str(cjp.cj_df_rows),str(cjp.cj_dataset_rows)])
    
    log_path = wd + "log/log.csv"
    if "log.csv" not in hdfs_client.list(wd+"log/"):
        log.write.csv(path=log_path, header="true", mode="overwrite", sep=";")
    else:
        prev_log = spark.read.csv(log_path, header=True, sep=";")
        prev_log.union(log).write(path=log_path, header="true", mode="overwrite", sep=";").csv(log_path)
        



main()






if "log.csv" not in hdfs_client.list(wd+"log/"):
        data_to_write = 'dt;loaded_rows;extracted_rows;processed_rows\n'+log + "\n"
        hdfs_client.write(log_path, data=bytes(data_to_write, encoding='utf8'), overwrite=True)
else:
        with hdfs_client.read(log_path) as reader:
            prev_log = reader.read()
        new_log = prev_log + bytes(log + "\n", encoding='utf8')
        hdfs_client.write(log_path, data=new_log, overwrite=True)
        # prev_log = hdfs_client.read(log_  path)
        # prev_log = spark.read.csv(log_path, header=True, sep=";").collect()
        # prev_log.union(log).coalesce(1).write.csv(path=log_path, header="true", mode="overwrite", sep=";")
        # hdfs_client.rename(log_path+"_1",log_path)


