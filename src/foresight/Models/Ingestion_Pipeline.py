import tensorflow as tf
import numpy as np
from google.cloud import storage
from tensorflow_datasets.core.utils import gcs_utils
#gcs_utils._is_gcs_disabled = True


tfrecord_dir = 'datasets_stacked/tfrecords_CS3/data/'

GCP_project = 'foresight-375620'
GCPClient = storage.Client(project=GCP_project)
bucket = GCPClient.bucket('frsght')

blobs = [b.name for b in bucket.list_blobs(prefix=tfrecord_dir)]
blobs = [f'gs://frsght/{b}' for b in blobs]

def read_tfexample(example):
    feature_map = {
        'embeddings': tf.io.FixedLenSequenceFeature([], tf.float32, default_value = 0.0, allow_missing = True),
        'height': tf.io.FixedLenFeature([], tf.int64),
        'width': tf.io.FixedLenFeature([], tf.int64),
        'label': tf.io.FixedLenFeature([], tf.int64),
    }

    sample = tf.io.parse_single_example(example, feature_map)

    feature = tf.reshape(sample['embeddings'], shape=[sample['height'], sample['width']])

    label = sample['label']

    return feature, label

dataset = tf.data.TFRecordDataset(blobs)
dataset = dataset.map(read_tfexample)

dataset = dataset.batch(128)
