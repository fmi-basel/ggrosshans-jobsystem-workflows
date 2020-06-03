'''preprocessing with tensorflow.
'''
import tensorflow as tf


def _normalize(img):
    '''normalizes to [-1, 1]
    '''
    lower = tf.reduce_min(img)
    upper = tf.reduce_max(img)
    return 2. * (img - lower) / (upper - lower) - 1.


def resample(img, shape):
    '''resamples to given shape.
    '''
    return tf.image.resize(img, shape, antialias=True)


class ProjectResampleNormalizePreprocessor:
    '''
    '''

    def __init__(self, model_input_size, n_channels=1):
        self.model_input_size = model_input_size
        self.n_channels = n_channels

    def _resize(self, img):
        return resample(img, self.model_input_size)

    def preprocess(self, img):
        '''
        '''
        img = tf.convert_to_tensor(img)
        tf.ensure_shape(img, (None, None, None, self.n_channels))
        # project along Z.
        img = tf.reduce_min(img, axis=0, keepdims=True)
        # resize to model target size.
        img = self._resize(img)
        # normalize to [-1, 1]
        return _normalize(img)
