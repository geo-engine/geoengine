import tensorflow as tf
from tensorflow import keras
import numpy as np
from tensorflow.keras import layers
from tensorflow.python.keras import callbacks
import matplotlib.pyplot
import keras.backend as K



physical_devices = tf.config.experimental.list_physical_devices('GPU')
print(physical_devices)
print("Num GPUs:", len(physical_devices))

tf.config.experimental.set_memory_growth(physical_devices[0], True)



model = keras.models.Sequential()

def initUnet(num_classes, id, batch_size):
    print(tf.__version__)
    inputs = keras.Input(shape=(512, 512, 7), batch_size=batch_size)

    ### [First half of the network: downsampling inputs] ###

    # Entry block
    x = layers.Conv2D(32, 3, strides=2, padding="same")(inputs)
    x = layers.BatchNormalization()(x)
    x = layers.Activation("relu")(x)

    previous_block_activation = x  # Set aside residual

    # Blocks 1, 2, 3 are identical apart from the feature depth.
    for filters in [64, 128, 256]:
        x = layers.Activation("relu")(x)
        x = layers.SeparableConv2D(filters, 3, padding="same")(x)
        x = layers.BatchNormalization()(x)

        x = layers.Activation("relu")(x)
        x = layers.SeparableConv2D(filters, 3, padding="same")(x)
        x = layers.BatchNormalization()(x)

        x = layers.MaxPooling2D(3, strides=2, padding="same")(x)

        # Project residual
        residual = layers.Conv2D(filters, 1, strides=2, padding="same")(
            previous_block_activation
        )
        x = layers.add([x, residual])  # Add back residual
        previous_block_activation = x  # Set aside next residual

    ### [Second half of the network: upsampling inputs] ###

    for filters in [256, 128, 64, 32]:
        x = layers.Activation("relu")(x)
        x = layers.Conv2DTranspose(filters, 3, padding="same")(x)
        x = layers.BatchNormalization()(x)

        x = layers.Activation("relu")(x)
        x = layers.Conv2DTranspose(filters, 3, padding="same")(x)
        x = layers.BatchNormalization()(x)

        x = layers.UpSampling2D(2)(x)

        # Project residual
        residual = layers.UpSampling2D(2)(previous_block_activation)
        residual = layers.Conv2D(filters, 1, padding="same")(residual)
        x = layers.add([x, residual])  # Add back residual
        previous_block_activation = x  # Set aside next residual

    # Add a per-pixel classification layer
    outputs = layers.Conv2D(num_classes, 3, activation="softmax", padding="same")(x)

    # Define the model
    global model 
    model = keras.Model(inputs, outputs)

    model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['sparse_categorical_accuracy'])
    model.summary()
    model.save('saved_model/{}'.format(id))
    print("Saved model under saved_model/{}".format(id))

def load(id):
    global model
    model = keras.models.load_model('saved_model/{}'.format(id))
    print('Loaded model from saved_model/{}'.format(id))

call = callbacks.CSVLogger('logs.csv', ';', append=True)
class_weight = {0: 1.0,
                1: 50.0,
                2: 50.0,
                3: 50.0,
                4: 50.0}
def pauls_awesome_loss(y_true, y_pred):
    return K.sqrt(K.sum(K.square(y_pred*K.cast(y_true>0, "float32") - y_pred)) / K.sum(K.cast(y_true>0, "float32") ))
def fit(X, y, batch_size):    
    global model
    matplotlib.pyplot.imsave('ir_test.png', X[0][:,:,0])
    matplotlib.pyplot.imsave('claas_test.png', y[0][:,:,0], vmin=0,vmax=3)
    print(y.shape)
    print(X.shape)
    #TODO check wether nan's present?
    #print("contains NaN's: {}".format(np.isnan(np.sum(X))))
    #print("contains inf's: {}".format(~np.all(~np.isinf(X))))
    #X = np.nan_to_num(X)
    #matplotlib.pyplot.imsave('train_claas.png', y[0][:,:,0], vmin=0,vmax=3)
    #matplotlib.pyplot.imsave('train_msg.png', X[0][:,:,1])
    
    global call
    model.fit(X, y, batch_size = batch_size, callbacks=[call])
    # if update >= 499:
    #     result = model.predict(X)
    #     result = result[0]
    #     classes = np.zeros((512,512))
    #     for i in range(0,512):
    #         for j in range(0,512):
    #             max = np.argmax(result[i,j,:], axis=0)
    #             classes[i,j]=max
    #     matplotlib.pyplot.imsave('train_prediction.png', classes, vmin=0,vmax=3)
    #     matplotlib.pyplot.imsave('train_claas.png', y[0][:,:,0], vmin=0,vmax=3)
    #     matplotlib.pyplot.imsave('train_msg.png', X[0][:,:,1])
    
        
    
    


def predict(X, y, batchsize):
    global model
    result = model.predict(X, batch_size = batchsize, verbose=1)
    #result_a = result[0]
    #classes = np.zeros((512,512))
    #for i in range(0,512):
    #    for j in range(0,512):
    #        max = np.argmax(result_a[i,j,:], axis=0)
    #        classes[i,j]=max

    #classes = result[0][:,:,0].argmax()
    #matplotlib.pyplot.imsave('prediction_val.png', classes, vmin=0,vmax=3)
    #matplotlib.pyplot.imsave('groundtruth_val.png', y[0][:,:,0], vmin=0,vmax=3)
    #matplotlib.pyplot.imsave('ir_val.png', X[0][:,:,0])
    
    #model.summary()
    #print(result.shape)
    #print(result[0][0][0])
    #print(result[0][0][1])
    #print(result[0][0][2])
    #print(result[0][0][3])
    
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

def test(X, y, t):
    global model
    #print(y[0][:,:,0].shape)
    r = X[0][:,:,0]
    g = X[0][:,:,1]
    b = X[0][:,:,2]
    #r[r < 0] = 0
    #g[g < 0] = 0
    #b[b < 0] = 0
    print(np.max(r))
    print(np.max(g))
    print(np.max(b))
    print(np.min(r))
    print(np.min(g))
    print(np.min(b))
    rgb = np.dstack((r, g ,b))
    matplotlib.pyplot.imsave('{}-RGB.png'.format(t), rgb)

    matplotlib.pyplot.imsave('{}-claas.png'.format(t), y[0][:,:,0], vmin=1,vmax=4)
    #for i in range(0,7):
    #    matplotlib.pyplot.imsave('{}-ir-number{}.png'.format(t,i+1), X[0][:,:,i])

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

def test_predict(X, y, batchsize, counter):
    global model
    result = model.predict(X, batch_size = batchsize)
    result = result[0]
    classes = np.zeros((512,512))
    for i in range(0,512):
        for j in range(0,512):
            max = np.argmax(result[i,j,:], axis=0)
            classes[i,j]=max
    matplotlib.pyplot.imsave('1-prediction.png', classes, vmin=0,vmax=3)
    matplotlib.pyplot.imsave('1.1-claas.png', y[0][:,:,0], vmin=0,vmax=3)