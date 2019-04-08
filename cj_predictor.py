import pandas
import numpy
import pickle
import json
import random
import os

from sklearn.model_selection import train_test_split, StratifiedShuffleSplit
from sklearn.metrics import roc_auc_score

import keras.preprocessing
from keras.layers import Concatenate
from keras.layers import MaxPooling1D
from keras.layers import Embedding, LSTM, Dense, PReLU, Dropout, BatchNormalization
from keras.layers import Conv1D
from keras.preprocessing import sequence
from keras.layers import Input, Dense, Reshape, Flatten, Dropout, multiply, GaussianNoise
from keras.layers import BatchNormalization, Activation, Embedding, ZeroPadding2D
from keras.models import Sequential, Model, load_model
import keras.backend as K

class CJ_Predictor:
    
    input_data = None
    model_path = None
    return_model = None
    
    model_path = None
    vocabulary_path = None
    model_weights_path = None
    
    train_auc = test_auc = test_auc_std = None
    
    def __init__(self, model_path, hdfs_client):
        print("Created Predictor Instance With Working Dir = {}".format(model_path))
        self.model_path = model_path
        self.vocabulary_path = model_path + "/vocabulary.pkl"
        self.model_weights_path = model_path + "/model.pkl"
        self.hdfs_client = hdfs_client
    
    def set_data(self, input_data):
        self.input_data = input_data
    
    def preprocess_data(self, model_update):
    
        data = self.input_data
        
        # Preprocess "TS Deltas" Sequence
        data['dt'] = data.dt.apply(lambda x: list(x)[0:len(x)-1])
        data.loc[:, 'dt'] = data.dt.apply(lambda r: [0]*(32-len(r)) + r if pandas.notna(numpy.array(r).any()) else [0]*32 )
        data.dt = data.dt.apply(lambda r: numpy.log(numpy.array(r)+1))
        Max = numpy.max(data.dt.apply(lambda r: numpy.max(r)))
        data.dt = data.dt.apply(lambda r: r / Max)
        data.dt = data.dt.apply(lambda r: r if len(r)==32 else r[-32:])
        
        # Preprocess Urls Sequence
        if model_update==True:
            tk = keras.preprocessing.text.Tokenizer(filters='', split=' ')
            tk.fit_on_texts(data.url.values)
            self.hdfs_client.write(self.vocabulary_path, data=pickle.dumps(tk), overwrite=True)
        else:
            with self.hdfs_client.read(self.vocabulary_path) as reader:
                tk = pickle.loads(reader.read())
            
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
    
    def optimize(self, batch_size):
        
        auc = []
        urls, dt, y, tk = self.preprocess_data(update_model = True)
        model = self.create_network(tk)
        
        cv_number = 0
        for cv_train_index, cv_test_index in StratifiedShuffleSplit(n_splits=5, train_size=0.25, test_size=0.10).split(y,y):
            cv_number += 1
            print("Fitting Model (CV={}) train length = {}, test length = {}".format(cv_number, len(cv_train_index), len(cv_test_index)))
            train = ([urls[cv_train_index], dt[cv_train_index]], y[cv_train_index])
            test = ([urls[cv_test_index], dt[cv_test_index]], y[cv_test_index])
            model.fit(train[0], train[1], epochs=1, batch_size=batch_size, shuffle = True)
            current_auc = roc_auc_score(test[1], model.predict(test[0]))
            print(current_auc)
            auc.append(current_auc)
        
        print("average AUC = {}, std AUC = {}".format(numpy.mean(auc), numpy.std(auc)))
        
        self.test_auc = round(numpy.mean(auc), 5)
        self.test_auc_std = round(numpy.std(auc), 5)
    
    def fit(self, update_model, batch_size):
        
        urls, dt, y, tk = self.preprocess_data(update_model)
        model = self.create_network(tk)
        
        train_data = ([urls, dt], y)
        scoring_data = [urls[self.input_data.target==0], dt[self.input_data.target==0]]
        
        if update_model:
            model.fit(train_data[0], train_data[1], epochs=1, batch_size=batch_size, shuffle = True)
            self.hdfs_client.write(self.model_weights_path, pickle.dumps(model), overwrite=True)
        else:
            with self.hdfs_client.read(self.model_weights_path) as reader:
                model = pickle.loads(reader.read())
        
        print("Scoring Data...")
        pred = model.predict(scoring_data)
        self.result = pandas.DataFrame({"fpc":self.input_data.fpc[self.input_data.target==0], "tpc":self.input_data.tpc[self.input_data.target==0], "return_score":pred.reshape(-1)})
        self.train_auc = round(roc_auc_score(train_data[1], model.predict(train_data[0], batch_size=batch_size)), 2)
        
        self.result['return_score'] = pandas.cut(self.result.return_score, 5, labels=['1','2','3','4','5'])
        
        
        
        return self.result