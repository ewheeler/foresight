from tensorflow import keras
import tensorflow as tf
from Ingestion_Pipeline import dataset

toy_model = tf.keras.Sequential([
    tf.keras.layers.Dense(100, input_shape = (512,50,), activation = 'relu'),
    tf.keras.layers.Dense(100, activation = 'relu'),
    tf.keras.layers.Dense(100, activation = 'relu'),
    tf.keras.layers.Dense(1, activation = 'sigmoid')
])

toy_model.compile(optimizer = 'adam', loss = 'binary_crossentropy', metrics = ['accuracy'])

