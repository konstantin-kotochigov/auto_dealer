import keras
from sklearn.model_selection import train_test_split
import numpy
from sklearn.metrics import roc_auc_score
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



def get_data(data):
    data['dt'] = data.dt.apply(lambda x: list(x)[0:len(x)-1])
    data.loc[:, 'dt'] = data.dt.apply(lambda r: [0]*(32-len(r)) + r if pandas.notna(numpy.array(r).any()) else [0]*32 )
    data.dt = data.dt.apply(lambda r: numpy.log(numpy.array(r)+1))
    Max = numpy.max(data.dt.apply(lambda r: numpy.max(r)))
    data.dt = data.dt.apply(lambda r: r / Max)
    data.dt = data.dt.apply(lambda r: r if len(r)==32 else r[-32:])
    X_train, X_test, y_train, y_test = train_test_split(data, data.target)
    tk = keras.preprocessing.text.Tokenizer()
    tk.fit_on_texts(data.url.values)
    urls_train = tk.texts_to_sequences(X_train.url)
    urls_train = sequence.pad_sequences(urls_train, maxlen=32)
    urls_test = tk.texts_to_sequences(X_test.url)
    urls_test = sequence.pad_sequences(urls_test, maxlen=32)
    dt_train = numpy.concatenate(X_train.dt.values).reshape((len(X_train), 32,1))
    dt_test = numpy.concatenate(X_test.dt.values).reshape((len(X_test), 32,1))
    return (urls_train, urls_test, dt_train, dt_test, y_train, y_test, tk)

def create_network(tk):
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
    return return_model

data = y_py[0:1000000].copy()
auc = []
for cv in range(1):
    urls_train, urls_test, dt_train, dt_test, y_train, y_test, tk = get_data(data)
    model = create_network(tk)
    model.fit([urls_train, dt_train], y_train, epochs=1, batch_size=1024, shuffle = True)
    current_auc = roc_auc_score(y_test, model.predict([urls_test, dt_test]))
    print(current_auc)
    auc.append(current_auc)

print("average AUC = {}, std AUC = {}".format(numpy.mean(auc), numpy.std(auc)))

