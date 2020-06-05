'''segmentation tasks with fully convolutional neural networks.

'''
import os

import luigi
from luigi.util import requires
import numpy as np
from tqdm import tqdm
from skimage.measure import block_reduce

import tensorflow as tf

from dlutils.prediction import predict_complete
from dlutils.prediction.runner import runner
from dlutils.models import load_model
from faim_luigi.targets.image_target import TiffImageTarget
from faim_luigi.tasks.collectors import ImageCollectorTask

from ggjw.tasks.logging import LGRunnerLoggingMixin
from ggjw.tasks.lgrunner.stop import StoppableTaskMixin


def add_progress(task, increment):
    task.trigger_event('event.lgrunner.progress.notification', task,
                       'add_percentage', increment)


@requires(ImageCollectorTask)
class BaseSegmentationModelPredictionTask(luigi.Task, LGRunnerLoggingMixin,
                                          StoppableTaskMixin):
    '''encapsulates model-unspecific parts of the segmentation
    task.

    '''
    output_folder = luigi.Parameter()
    verbose = luigi.BoolParameter(default=False)

    accepts_messages = True

    def _get_iterable(self):
        '''returns iterable over input and target pairs.
        '''
        iterable = [(inp, target)
                    for inp, target in zip(self.input(), self.output())
                    if not target.exists()]

        if self.verbose:
            iterable = tqdm(iterable,
                            ncols=80,
                            desc='Running segmentation model')
        return iterable

    @property
    def _fraction(self):
        return 100. / len(self.input())

    def _report_initial(self, iterable):
        '''reports number of inputs to process and updates
        the progressbar.
        '''
        num_inputs = len(self.input())
        num_done = len(self.input()) - len(iterable)
        self.log_info(
            'Found {} input images. {} are already processed.'.format(
                num_inputs, num_done))

        add_progress(self, num_done * self._fraction)

    def output(self):
        '''
        '''
        if not self.input():
            raise ValueError('No input images provided!')

        def _get_fname(img_path):
            return os.path.splitext(os.path.basename(img_path))[0] + '.tif'

        return [
            TiffImageTarget(
                os.path.join(self.output_folder,
                             _get_fname(input_target.path)))
            for input_target in self.input()
        ]


class RunBinarySegmentationModelPredictionTask(
        BaseSegmentationModelPredictionTask):
    '''Applies a given (deployed) model to a collection of images.
    '''

    model_folder = luigi.Parameter()

    model_signature = luigi.Parameter(default='serve')
    model_output_name = luigi.Parameter(default='output_0')

    def load_predictor(self):
        ''' loads model and returns function to call the respective
        signature. The signature is expected to be a complete serve,
        i.e. include preprocessing and potential postprocessing.
        '''
        self._model = tf.saved_model.load(self.model_folder)
        self.log_info('Loaded model from {}.'.format(self.model_folder))
        return self._model.signatures[self.model_signature]

    def run(self):
        '''
        '''
        predict = self.load_predictor()
        iterable = self._get_iterable()
        self._report_initial(iterable)
        self.log_info('Starting to process {} images.'.format(len(iterable)))

        for input_handle, target in iterable:

            self.raise_if_interrupt_signal()

            # load data
            try:
                img = input_handle.load()
            except Exception as err:
                self.log_error(
                    'Could not load image from {}. Error: {}'.format(
                        input_handle.path, err))
                continue

            # process
            try:
                prediction = predict(
                    tf.convert_to_tensor(img))[self.model_output_name]
            except Exception as err:
                self.log_error('Could not process target {}. Error: {}'.format(
                    target.path, err))
                continue

            # save
            try:
                target.save(prediction.numpy(), compress=9)
            except Exception as err:
                self.log_error('Could not save target. Error: {}'.format(err))

            # update progress
            add_progress(self, self._fraction)

        self.log_info('{} done. Segmented {} images.'.format(
            self.__class__.__name__,
            sum(target.exists() for _, target in iterable)))


class RunBinarySegmentationModelPredictionTaskV0(
        BaseSegmentationModelPredictionTask):
    '''Applies the given "old-style" model to a collection of images.

    NOTE this workflow projects from 3D to 2D!

    '''
    downsampling = luigi.IntParameter(default=1)
    model_folder = luigi.Parameter()
    model_weights_fname = luigi.Parameter()
    auto_rescale = luigi.BoolParameter(default=False)

    patch_size = luigi.IntParameter(
        default=None, visibility=luigi.parameter.ParameterVisibility.HIDDEN)
    patch_overlap = luigi.IntParameter(
        default=10, visibility=luigi.parameter.ParameterVisibility.HIDDEN)
    batch_size = luigi.IntParameter(
        default=1, visibility=luigi.parameter.ParameterVisibility.HIDDEN)

    @property
    def _patch_size(self):
        '''generates the patch size tuple or returns None
        if undefined.

        '''
        if self.patch_size is None:
            return self.patch_size
        return tuple(self.patch_size for _ in range(2))

    def preprocess_fn(self, image):
        '''
        '''
        img = image.min(axis=0)

        if self.auto_rescale:
            lower, upper = np.percentile(img.flat, (0, 100))
            LOW, HIGH = 0, 8000  # Consider replacing this hack with a proper solution.
            factor = (HIGH - LOW) / (upper - lower)
            img = (img.astype('float32') - lower) * factor + LOW
        if self.downsampling >= 2:
            img = block_reduce(
                img, tuple(int(self.downsampling) for _ in range(img.ndim)),
                np.min)
        return img

    def run(self):
        '''
        '''
        model = load_model(
            os.path.join(self.model_folder, self.model_weights_fname))
        self.log_info('Loaded model from {}.'.format(self.model_folder))

        iterable = self._get_iterable()
        self._report_initial(iterable)


        # NOTE loading, processing and saving are done with multiple
        # threads and two queues. Projection and FCN are sequential
        # and could therefore be optimized.

        def loader_fn(input_target, output_target):
            '''
            '''
            try:
                return input_target.load(), output_target
            except Exception as err:
                self.log_error(
                    'Could not load image from {}. Error: {}'.format(
                        input_target.path, err))

        def processor_fn(image, target):
            '''
            '''
            # check if an interrupt has been received.
            self.raise_if_interrupt_signal()
            try:
                prediction = (
                    predict_complete(model,
                                     self.preprocess_fn(image),
                                     patch_size=self._patch_size,
                                     border=self.patch_overlap,
                                     batch_size=self.batch_size)['fg'] *
                    255).astype(np.uint8)
                return prediction, target
            except Exception as err:
                self.log_error('Could not process target {}. Error: {}'.format(
                    target.path, err))

        def saver_fn(prediction, target):
            '''
            '''
            try:
                target.save(prediction)
            except Exception as err:
                self.log_error('Could not save target {}. Error: {}'.format(
                    target.path, err))

            add_progress(self, self._fraction)

        self.log_info('Starting to process {} images.'.format(len(iterable)))

        runner(loader_fn, processor_fn, saver_fn, iterable, queue_maxsize=5)
        self.log_info('{} done. Segmented {} images.'.format(
            self.__class__.__name__, len(iterable)))
