import tensorflow as tf
from tensorflow import keras
import numpy as np
from tensorflow.keras import layers
from tensorflow.python.keras.backend import dropout
from tensorflow.keras.callbacks import LambdaCallback

physical_devices = tf.config.experimental.list_physical_devices('GPU')
print(physical_devices)
print("Num GPUs:", len(physical_devices))



tf.config.experimental.set_memory_growth(physical_devices[0], True)

model = keras.models.Sequential()

def initUnet(num_classes, id, batch_size):
    start_neurons = 32
    inputs = keras.Input(shape=(512, 512, 1), batch_size=batch_size)

    conv1 = layers.Conv2D(start_neurons, (3,3), activation="relu", padding='same')(inputs)
    conv1 = layers.Conv2D(start_neurons, (3,3), activation='relu', padding='same')(conv1)
    pool1 = layers.MaxPooling2D((2,2))(conv1)
    pool1 = layers.Dropout(0.25)(pool1)

    conv2 = layers.Conv2D(start_neurons * 2, (3, 3), activation="relu", padding="same")(pool1)
    conv2 = layers.Conv2D(start_neurons * 2, (3, 3), activation="relu", padding="same")(conv2)
    pool2 = layers.MaxPooling2D((2, 2))(conv2)
    pool2 = layers.Dropout(0.5)(pool2)

    conv3 = layers.Conv2D(start_neurons * 4, (3, 3), activation="relu", padding="same")(pool2)
    conv3 = layers.Conv2D(start_neurons * 4, (3, 3), activation="relu", padding="same")(conv3)
    pool3 = layers.MaxPooling2D((2, 2))(conv3)
    pool3 = layers.Dropout(0.5)(pool3)

    conv4 = layers.Conv2D(start_neurons * 8, (3, 3), activation="relu", padding="same")(pool3)
    conv4 = layers.Conv2D(start_neurons * 8, (3, 3), activation="relu", padding="same")(conv4)
    pool4 = layers.MaxPooling2D((2, 2))(conv4)
    pool4 = layers.Dropout(0.5)(pool4)

    # Middle
    convm = layers.Conv2D(start_neurons * 16, (3, 3), activation="relu", padding="same")(pool4)
    convm = layers.Conv2D(start_neurons * 16, (3, 3), activation="relu", padding="same")(convm)

    deconv4 = layers.Conv2DTranspose(start_neurons * 8, (3, 3), strides=(2, 2), padding="same")(convm)
    uconv4 = layers.concatenate([deconv4, conv4])
    uconv4 = layers.Dropout(0.5)(uconv4)
    uconv4 = layers.Conv2D(start_neurons * 8, (3, 3), activation="relu", padding="same")(uconv4)
    uconv4 = layers.Conv2D(start_neurons * 8, (3, 3), activation="relu", padding="same")(uconv4)

    deconv3 = layers.Conv2DTranspose(start_neurons * 4, (3, 3), strides=(2, 2), padding="same")(uconv4)
    uconv3 = layers.concatenate([deconv3, conv3])
    uconv3 = layers.Dropout(0.5)(uconv3)
    uconv3 = layers.Conv2D(start_neurons * 4, (3, 3), activation="relu", padding="same")(uconv3)
    uconv3 = layers.Conv2D(start_neurons * 4, (3, 3), activation="relu", padding="same")(uconv3)

    deconv2 = layers.Conv2DTranspose(start_neurons * 2, (3, 3), strides=(2, 2), padding="same")(uconv3)
    uconv2 = layers.concatenate([deconv2, conv2])
    uconv2 = layers.Dropout(0.5)(uconv2)
    uconv2 = layers.Conv2D(start_neurons * 2, (3, 3), activation="relu", padding="same")(uconv2)
    uconv2 = layers.Conv2D(start_neurons * 2, (3, 3), activation="relu", padding="same")(uconv2)

    deconv1 = layers.Conv2DTranspose(start_neurons * 1, (3, 3), strides=(2, 2), padding="same")(uconv2)
    uconv1 = layers.concatenate([deconv1, conv1])
    uconv1 = layers.Dropout(0.5)(uconv1)
    uconv1 = layers.Conv2D(start_neurons * 1, (3, 3), activation="relu", padding="same")(uconv1)
    uconv1 = layers.Conv2D(start_neurons * 1, (3, 3), activation="relu", padding="same")(uconv1)

    output_layer = layers.Conv2D(num_classes, (1,1), padding='same', activation='softmax')(uconv1)

    global model
    model = keras.Model(inputs, output_layer)

    optimizer=keras.optimizers.Adam(lr=0.01)

    model.compile(optimizer='adam', loss="sparse_categorical_crossentropy", metrics=['accuracy'])

    model.summary()
    model.save('saved_model/{}'.format(id))
    print("Saved model under saved_model/{}".format(id))

def load(id):
    global model
    model = keras.models.load_model('saved_model/{}'.format(id))
    print('Loaded model from saved_model/{}'.format(id))

print_weights = LambdaCallback(on_epoch_end=lambda batch, logs: print(model.layers[1].get_weights()))

def fit(X, y, batch_size):    
    global model
    #print(y.shape)
    #print(X.shape)
    #TODO check wether nan's present?
    #print("contains NaN's: {}".format(np.isnan(np.sum(X))))
    #print("contains inf's: {}".format(~np.all(~np.isinf(X))))
    #X = np.nan_to_num(X)

    
    
    model.fit(X, y, batch_size = batch_size)


def predict(X):
    global model
    result = model.predict(X, batch_size=1)
    #model.summary()
    print(result.shape)
    print(result[0][0][0])
    print(result[0][0][255])
    print(result[0][255][0])
    print(result[0][255][255])
    return result

def validate(X, y, batch_size):
    global model
    score = model.evaluate(x=X, y=y, batch_size=batch_size)
    #print(score)
    return np.array([score[0], score[1]])
def save(id):
    global model
    model.save('saved_model/{}'.format(id))
    print("Saved model under saved_model/{}".format(id))


def get_model_memory_usage(batch_size):
    global model
    try:
        from keras import backend as K
    except:
        from tensorflow.keras import backend as K

    shapes_mem_count = 0
    internal_model_mem_count = 0
    for l in model.layers:
        layer_type = l.__class__.__name__
        if layer_type == 'Model':
            internal_model_mem_count += get_model_memory_usage(batch_size, l)
        single_layer_mem = 1
        out_shape = l.output_shape
        if type(out_shape) is list:
            out_shape = out_shape[0]
        for s in out_shape:
            if s is None:
                continue
            single_layer_mem *= s
        shapes_mem_count += single_layer_mem

    trainable_count = np.sum([K.count_params(p) for p in model.trainable_weights])
    non_trainable_count = np.sum([K.count_params(p) for p in model.non_trainable_weights])

    number_size = 4.0
    if K.floatx() == 'float16':
        number_size = 2.0
    if K.floatx() == 'float64':
        number_size = 8.0

    total_memory = number_size * (batch_size * shapes_mem_count + trainable_count + non_trainable_count)
    gbytes = np.round(total_memory / (1024.0 ** 3), 3) + internal_model_mem_count
    print("Model needs {} GB's of Memory".format(gbytes))
