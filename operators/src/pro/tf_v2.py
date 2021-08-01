import tensorflow as tf
from tensorflow import keras
import numpy as np
from tensorflow.keras import layers


physical_devices = tf.config.experimental.list_physical_devices('GPU')
print(physical_devices)
print("Num GPUs:", len(physical_devices))



tf.config.experimental.set_memory_growth(physical_devices[0], True)



model = keras.models.Sequential()

def initUnet(num_classes, id):
    print(tf.__version__)
    inputs = keras.Input(shape=(512, 512, 7), batch_size=1)

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
    model.compile(optimizer="rmsprop", loss="sparse_categorical_crossentropy")
    model.summary()
    model.save('saved_model/{}'.format(id))
    print("Saved model under saved_model/{}".format(id))

def load(id):
    global model
    model = keras.models.load_model('saved_model/{}'.format(id))
    print('Loaded model from saved_model/{}'.format(id))


def fit(X, y):    
    global model
    print(y.shape)
    print(X.shape)
    
    model.fit(X, y, batch_size = 1)


def predict(X):
    global model
    result = model.predict(X)
    model.summary()
    print(result.shape)
    print(result[0][0])
    print(result[0][1])
    print(result[0][2])
    print(result[0][3])
    return result

def save(id):
    global model
    model.save('saved_model/{}'.format(id))
    print("Saved model under saved_model/{}".format(id))
