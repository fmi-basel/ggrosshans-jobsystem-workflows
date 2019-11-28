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


@requires(ImageCollectorTask)
class RunBinarySegmentationModelPredictionTask(luigi.Task):
    '''Applies the given model to a collection of images.

    NOTE this workflow projects from 3D to 2D!

    '''
    downsampling = luigi.IntParameter(default=1)
    model_folder = luigi.Parameter()
    model_weights_fname = luigi.Parameter()
    output_folder = luigi.Parameter()
    verbose = luigi.BoolParameter(default=True)

    def preprocess_fn(self, image):
        '''
        '''
        img = image.min(axis=0)
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

        def loader_fn(input_target, output_target):
            '''
            '''
            return input_target.load(), output_target

        def processor_fn(image, target):
            '''
            '''
            prediction = (predict_complete(
                model, self.preprocess_fn(image), batch_size=1)['fg'] *
                          255).astype(np.uint8)
            return prediction, target

        def saver_fn(prediction, target):
            '''
            '''
            target.save(prediction)

        iterable = [(inp, target)
                    for inp, target in zip(self.input(), self.output())
                    if not target.exists()]

        if self.verbose:
            iterable = tqdm(iterable,
                            ncols=80,
                            desc='Running segmentation model')

        runner(loader_fn, processor_fn, saver_fn, iterable, queue_maxsize=5)

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
