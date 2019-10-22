import os

import luigi

import numpy as np
from skimage.measure import block_reduce

from dlutils.prediction import predict_complete
from dlutils.prediction.runner import runner

from faim_luigi.targets.image_target import TiffImageTarget


class RunBinarySegmentationModelPredictionTask(luigi.Task):
    '''Applies the given model to a series of images.

    '''
    downsampling = luigi.IntParameter(default=1)
    model_folder = luigi.Parameter()
    model_weights_fname = luigi.Parameter()

    image_paths = luigi.ListParameter()
    output_folder = luigi.Parameter()

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
        from tqdm import tqdm
        from skimage.external.tifffile import imread
        from dlutils.models import load_model

        model = load_model(
            os.path.join(self.model_folder, self.model_weights_fname))

        def loader_fn(image_path, target):
            '''
            '''
            return imread(image_path), target

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

        runner(loader_fn,
               processor_fn,
               saver_fn,
               tqdm([(path, target)
                     for path, target in zip(self.image_paths, self.output())
                     if not target.exists()],
                    ncols=80),
               queue_maxsize=5)

    def output(self):
        '''
        '''
        if not self.image_paths:
            raise ValueError('No input images provided!')

        def _get_fname(img_path):
            return os.path.splitext(os.path.basename(img_path))[0] + '.tif'

        return [
            TiffImageTarget(
                os.path.join(self.output_folder, _get_fname(img_path)))
            for img_path in self.image_paths
        ]
