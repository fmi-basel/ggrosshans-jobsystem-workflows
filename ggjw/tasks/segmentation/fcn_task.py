'''segmentation tasks with fully convolutional neural networks.

'''
import os

import luigi
from luigi.util import requires
import numpy as np
from tqdm import tqdm
from skimage.measure import block_reduce

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
class RunBinarySegmentationModelPredictionTask(luigi.Task,
                                               LGRunnerLoggingMixin,
                                               StoppableTaskMixin):
    '''Applies the given model to a collection of images.

    NOTE this workflow projects from 3D to 2D!

    '''
    downsampling = luigi.IntParameter(default=1)
    model_folder = luigi.Parameter()
    model_weights_fname = luigi.Parameter()
    output_folder = luigi.Parameter()
    verbose = luigi.BoolParameter(default=False)

    patch_size = luigi.IntParameter(
        default=None, visibility=luigi.parameter.ParameterVisibility.HIDDEN)
    patch_overlap = luigi.IntParameter(
        default=10, visibility=luigi.parameter.ParameterVisibility.HIDDEN)
    batch_size = luigi.IntParameter(
        default=1, visibility=luigi.parameter.ParameterVisibility.HIDDEN)

    accepts_messages = True

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
        if self.downsampling >= 2:
            img = block_reduce(img,
                               tuple(
                                   int(self.downsampling)
                                   for _ in range(img.ndim)), np.min)
        return img

    def run(self):
        '''
        '''
        model = load_model(
            os.path.join(self.model_folder, self.model_weights_fname))
        self.log_info('Loaded model from {}.'.format(self.model_folder))

        iterable = [(inp, target)
                    for inp, target in zip(self.input(), self.output())
                    if not target.exists()]
        fraction = 100. / len(self.input())
        add_progress(self, (len(self.input()) - len(iterable)) * fraction)

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
                prediction = (predict_complete(
                    model,
                    self.preprocess_fn(image),
                    patch_size=self._patch_size,
                    border=self.patch_overlap,
                    batch_size=self.batch_size)['fg'] * 255).astype(np.uint8)
                return prediction, target
            except Exception as err:
                self.log_error('Could not process target {}. Error: {}'.format(target.path, err))

        def saver_fn(prediction, target):
            '''
            '''
            try:
                target.save(prediction)
            except Exception as err:
                self.log_error('Could not save target {}. Error: {}'.format(target.path, err))

            add_progress(self, fraction)

        #iterable = [(inp, target)
        #            for inp, target in zip(self.input(), self.output())
        #            if not target.exists()]

        if self.verbose:
            iterable = tqdm(iterable,
                            ncols=80,
                            desc='Running segmentation model')

        self.log_info('Starting to process {} images.'.format(len(iterable)))

        runner(loader_fn, processor_fn, saver_fn, iterable, queue_maxsize=5)
        self.log_info('{} done. Segmented {} images.'.format(
            self.__class__.__name__, len(iterable)))

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
