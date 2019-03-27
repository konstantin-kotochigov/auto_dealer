import sys
from pyspark.sql import SparkSession



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
    
    dryrun = sys.argv[1]
    print("dryrun = {}".format(dryrun))
    
    spark = SparkSession.builder.appName('analytical_attributes').getOrCreate()
    
    # Load Data
    cjp = CJ_Loader(spark)
    cjp.set_organization("57efd33d-aaa5-409d-89ce-ff29a86d78a5")
    cjp.load_cj(ts_from=(2018,12,1), ts_to=(2018,12,31))
    cjp.cj_data.createOrReplaceTempView('cj')
    cjp.extract_attributes()
    cjp.process_attributes()
    data = cjp.cj_dataset
    
    # Make Model
    predictor = Predictor()
    predictor.set_data(data)
    # predictor.optimize()
    result = predictor.fit()
    
    # Make Delta
    df = spark.createDataFrame(result)
    dm = DeltaMaker("57efd33d-aaa5-409d-89ce-ff29a86d78a5", "model_update", "http://159.69.60.183:50070", "schema.avsc")
    
    # Publish Delta
    if dryrun == "1":
        send_update = False
    dm.make_delta(df, mapping, send_update=False)
        



main()









