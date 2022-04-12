import tensorflow as tf
from tensorflow import keras
import numpy as np
from tensorflow.keras import layers
from tensorflow.python.keras import callbacks
import matplotlib.pyplot
import keras.backend as K
#from tensorflow.keras import mixed_precision
#mixed_precision.set_global_policy(policy="mixed_float16")



physical_devices = tf.config.experimental.list_physical_devices('GPU')
print(physical_devices)
print("Num GPUs:", len(physical_devices))
#print(f"Global Policy: {mixed_precision.global_policy()}")

tf.config.experimental.set_memory_growth(physical_devices[0], True)



model = keras.models.Sequential()
update = 50
def weighted_standard_loss(weights):
    def my_loss(y_true, y_pred):
        weights_con = tf.constant(weights)
        #nop = tf.size(y_true)
        y_true = K.squeeze(K.one_hot(K.cast(y_true, np.int32), 5), axis=3)

        y_pred = K.clip(y_pred, K.epsilon(), 1)

        logs = K.log(y_pred)

        m_full_weights = K.dot(y_true, weights_con)
        #tf.print(m_full_weights * logs)
        return -K.mean(m_full_weights * logs)
        #y_true = tf.squeeze(tf.one_hot(tf.cast(y_true, np.int32),  depth = 5), axis=3)
        tf.print(y_true.shape)

        #tf.print(tf.math.multiply(tf.cast(y_true, np.float32), weights_con))
        #inter = tf.math.multiply(tf.math.multiply(tf.cast(y_true, np.float32), weights_con), tf.math.#log(tf.clip_by_value(y_pred, 1e-10, 1.0)))
        #tf.print(inter)
        #return K.mean(tf.math.reduce_sum(tf.math.negative(inter)))

    return my_loss
    
def rwwce(marginal_cost_false_negativ, marginal_cost_false_positive):
    def my_loss(y_true, y_pred):
        fn_wt = K.constant(marginal_cost_false_negativ)
        fp_wt = K.constant(marginal_cost_false_positive)

        y_true = K.squeeze(K.one_hot(K.cast(y_true, np.int32), 5), axis=3)
        #tf.print(y_true.shape)
        y_pred = K.clip(y_pred, K.epsilon(), 1-K.epsilon())

        logs = K.log(y_pred)
        logs_1_sub = K.log(1-y_pred)


        m_full_fn_weights = K.dot(y_true, fn_wt)
        m_full_fp_weights = K.dot(y_true, fp_wt)

        return -K.mean(m_full_fn_weights * logs + m_full_fp_weights * logs_1_sub)

    return my_loss


def my_loss(y_true, y_pred):
    class_weights = tf.constant([[1.0, 1.0, 1.0, 1.0, 1.0]])
    # deduce weights for batch samples based on their true label
    onehot_labels = tf.squeeze(tf.one_hot(tf.cast(y_true, np.int32), depth=5), axis=3)
    weights = tf.reduce_sum(class_weights * onehot_labels, axis=1)
    # compute your (unweighted) softmax cross entropy loss
    unweighted_losses = tf.nn.softmax_cross_entropy_with_logits(onehot_labels, y_pred)
    # apply the weights, relying on broadcasting of the multiplication
    weighted_losses = unweighted_losses * weights
    # reduce the result to get your final loss
    loss = tf.reduce_mean(weighted_losses)
    return loss

def initUnet(num_classes, id, batch_size):
    print(tf.__version__)
    inputs = keras.Input(shape=(512, 512, 6), batch_size=batch_size, dtype=tf.dtypes.float16)

    ### [First half of the network: downsampling inputs] ###

    # Entry block
    x = layers.layers.Conv2D(32, 3, strides=2, padding="same")(inputs)
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
        residual = layers.layers.Conv2D(filters, 1, strides=2, padding="same")(
            previous_block_activation
        )
        x = layers.add([x, residual])  # Add back residual
        previous_block_activation = x  # Set aside next residual

    ### [Second half of the network: upsampling inputs] ###

    for filters in [256, 128, 64, 32]:
        x = layers.Activation("relu")(x)
        x = layers.layers.Conv2DTranspose(filters, 3, padding="same")(x)
        x = layers.BatchNormalization()(x)

        x = layers.Activation("relu")(x)
        x = layers.layers.Conv2DTranspose(filters, 3, padding="same")(x)
        x = layers.BatchNormalization()(x)

        x = layers.UpSampling2D(2)(x)

        # Project residual
        residual = layers.UpSampling2D(2)(previous_block_activation)
        residual = layers.layers.Conv2D(filters, 1, padding="same")(residual)
        x = layers.add([x, residual])  # Add back residual
        previous_block_activation = x  # Set aside next residual

    # Add a per-pixel classification layer
    outputs = layers.layers.Conv2D(num_classes, 3, activation="softmax", padding="same", dtype=tf.dtypes.float32)(x)

    # Define the model
    global model 
    model = keras.Model(inputs, outputs)

    fn = np.array([[1.0, 0.0, 0.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0, 0.0], [0.0, 0.0, 1.0, 0.0, 0.0], [0.0, 0.0, 0.0, 1.0, 0.0], [0.0, 0.0, 0.0, 0.0, 5.0]])
    fp = np.array([[0.0, 999999.9, 999999.9, 999999.9, 999999.9], [0.1, 0.0, 1.0, 1.0, 1.0], [0.1, 1.0, 0.0, 1.0, 1.0], [0.1, 99.0, 99.0, 0.0, 99.0], [0.1, 1.0, 1.0, 1.0, 0.0]])

    model.compile(optimizer='adam', loss=rwwce(fn, fp), metrics=['sparse_categorical_accuracy'])
    model.summary()
    model.save('saved_model/{}'.format(id))
    print("Saved model under saved_model/{}".format(id))

def load(id):
    global model
    fn = np.array([[1.0, 0.0, 0.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0, 0.0], [0.0, 0.0, 1.0, 0.0, 0.0], [0.0, 0.0, 0.0, 1.0, 0.0], [0.0, 0.0, 0.0, 0.0, 1.0]])
    fp = np.array([[0.0, 999999.9, 999999.9, 999999.9, 999999.9], [1.0, 0.0, 10.0, 10.0, 1.0], [1.0, 1.0, 0.0, 1.0, 1.0], [1.0, 1.0, 1.0, 0.0, 1.0], [1.0, 1.0, 1.0, 1.0, 0.0]])
    #model = keras.models.load_model('saved_model/{}'.format(id), custom_objects={ 'loss': rwwce(fn, fp)})
    model = keras.models.load_model('saved_model/{}'.format(id), compile=False)
    print('Loaded model from saved_model/{}'.format(id))

call = callbacks.CSVLogger('logs.csv', ';', append=True)
class_weight = tf.constant([0,1,1,1,1])

def initUnet2(num_classes, id, batch_size):
    print(tf.__version__)
    inputs = keras.Input(shape=(512, 512, 11), batch_size=batch_size)

    conv1 = layers.Conv2D(32, 3,activation = 'relu', padding = 'same')(inputs)
    conv1 = layers.Conv2D(32, 3,activation = 'relu', padding = 'same')(conv1)
    pool1 = layers.Conv2D(32, 3, strides=(2,2), padding = 'same')(conv1)
    print('pool1')
    print(pool1.shape)
    conv2 = layers.Conv2D(64, 3, activation = 'relu', padding = 'same')(pool1)
    conv2 = layers.Conv2D(64, 3, activation = 'relu', padding = 'same')(conv2)
    pool2 = layers.Conv2D(64, 3, strides=(2,2), padding = 'same')(conv2)
    print('pool2')
    print(pool2.shape)
    conv3 = layers.Conv2D(128, 3, activation = 'relu', padding = 'same')(pool2)
    conv3 = layers.Conv2D(128, 3, activation = 'relu', padding = 'same')(conv3)
    pool3 = layers.Conv2D(128, 3, strides=(2,2), padding = 'same')(conv3)
    print('pool3')
    print(pool3.shape)
    conv4 = layers.Conv2D(256, 3, activation = 'relu', padding = 'same')(pool3)
    conv4 = layers.Conv2D(256, 3, activation = 'relu', padding = 'same')(conv4)
    drop4 = layers.Dropout(0.5)(conv4)
    pool4 = layers.Conv2D(256, 3, strides=(2,2), padding = 'same')(drop4)
    print('pool4')
    print(pool4.shape)
    conv5 = layers.Conv2D(512, 3, activation = 'relu', padding = 'same')(pool4)
    conv5 = layers.Conv2D(512, 3, activation = 'relu', padding = 'same')(conv5)
    drop5 = layers.Dropout(0.5)(conv5)
    print('drop5')
    print(drop5.shape)
    up6 = layers.Conv2DTranspose(256, 2, strides=(2,2), padding= 'same', activation = 'relu')(drop5)
    print('drop4')
    print(drop4.shape)
    crop4 = layers.Cropping2D(cropping=1)(drop4)
    merge6 = layers.concatenate([drop4, up6])
    conv6 = layers.Conv2D(256, 3, activation = 'relu', padding = 'same')(merge6)
    conv6 = layers.Conv2D(256, 3, activation = 'relu', padding = 'same')(conv6)
    print('conv6')
    print(conv6.shape)
    up7 = layers.Conv2DTranspose(128, 2, strides=(2,2), padding= 'same', activation = 'relu')(conv6)
    print('CONV3')
    print(conv3.shape)
    crop3 = layers.Cropping2D(cropping=16)(conv3)
    merge7 = layers.concatenate([conv3, up7])
    conv7 = layers.Conv2D(128, 3, activation = 'relu', padding = 'same')(merge7)
    conv7 = layers.Conv2D(128, 3, activation = 'relu', padding = 'same')(conv7)
    print('conv7')
    print(conv7.shape)
    up8 = layers.Conv2DTranspose(64, 2, strides=(2,2), padding= 'same', activation = 'relu')(conv7)
    crop2 = layers.Cropping2D(cropping=40)(conv2)
    merge8 = layers.concatenate([conv2, up8])
    conv8 = layers.Conv2D(64, 3, activation = 'relu', padding = 'same')(merge8)
    conv8 = layers.Conv2D(64, 3, activation = 'relu', padding = 'same')(conv8)
    print('conv8')
    print(conv8.shape)
    up9 = layers.Conv2DTranspose(32, 2, strides=(2,2), padding= 'same', activation = 'relu')(conv8)
    crop1 = layers.Cropping2D(cropping=88)(conv1)
    merge9 = layers.concatenate([conv1,up9])
    conv9 = layers.Conv2D(32, 3, activation = 'relu', padding = 'same')(merge9)
    conv9 = layers.Conv2D(32, 3, activation = 'relu', padding = 'same')(conv9)
    print('conv9')
    print(conv9.shape)
    #_, _, out_classes = input_size
#    conv9 = layers.Conv2D(5, 1, padding = 'same', activation = softMaxAxis(axis=channel_axis),  data_format=data_format)(conv9)
    conv9 = layers.Conv2D(5, 1, padding = 'same', activation='softmax')(conv9)

    # Define the model
    global model 
    model = keras.Model(inputs, conv9)

    fn = np.array([[1.0, 0.0, 0.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0, 0.0], [0.0, 0.0, 1.0, 0.0, 0.0], [0.0, 0.0, 0.0, 1.0, 0.0], [0.0, 0.0, 0.0, 0.0, 1.0]])
    fp = np.array([[0.0, 9999.9, 9999.9, 9999.9, 9999.9], [1.0, 0.0, 1.0, 1.0, 1.0], [1.0, 1.0, 0.0, 1.0, 1.0], [1.0, 1.0, 1.0, 0.0, 1.0], [1.0, 1.0, 1.0, 1.0, 0.0]])
 
    model.compile(optimizer=tf.keras.optimizers.Adam(), loss = 'sparse_categorical_crossentropy', metrics=['sparse_categorical_accuracy'])
    model.summary()
    model.save('saved_model/{}'.format(id))
    print("Saved model under saved_model/{}".format(id))

def fit(X, y):    
    global model
    global update
    
    global call
    model.fit(X, y, callbacks=[call])
    update = update + 1
    if update >= 30:
        update = 0
        result = model.predict(X)
        result = result[0]
        classes = np.zeros((512,512))
        for i in range(0,512):
            for j in range(0,512):
                max = np.argmax(result[i,j,:], axis=0)
                classes[i,j]=max

        clist = ["purple", "blue", "red", "green", "yellow"]
        cmap = matplotlib.colors.ListedColormap(clist)
        matplotlib.pyplot.imsave('train_prediction.png', classes, vmin=0,vmax=4, cmap=cmap)
        matplotlib.pyplot.imsave('train_claas.png', y[0][:,:,0], vmin=0,vmax=4, cmap=cmap)
        matplotlib.pyplot.imsave('train_msg.png', X[0][:,:,1])
    
        
    
    


def predict(X, batchsize):
    global model
    result = model.predict(X, batch_size = batchsize, verbose=1)
    return result

def sameate(X, y, batch_size):
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