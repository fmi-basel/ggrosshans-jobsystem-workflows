import os
import glob

import luigi

from ..tasks.segmentation.fcn_task import RunBinarySegmentationModelPredictionTask
from . import JobSystemWorkflow


class WormSegmentationFromDICWorkflow(luigi.WrapperTask, JobSystemWorkflow):
    '''
    '''
    input_folder = luigi.Parameter()
    '''base folder containing the images to be processed.

    '''
    input_pattern = luigi.Parameter()
    '''file name pattern that matches all images that need to be
    processed.

    All files that match:

      input_folder +input_pattern

    will be processed.

    Example
    -------

    '*DIC-GFP3*.stk' will match all images containing DIC-GFP3, while
    excluding all others.

    '''

    output_folder = luigi.Parameter()
    '''output folder into which the segmentations will be written.

    '''

    # TODO consider removing
    downsampling = luigi.IntParameter(default=1)
    '''downsampling factor for the images.

    '''

    # TODO consider removing
    model_folder = luigi.Parameter()
    '''folder containing model to be applied.

    '''

    # TODO consider removing
    model_weights_fname = luigi.Parameter()
    '''filename of weights. Must exist in model_folder.

    '''

    task_namespace = 'ggrosshans'

    def requires(self):
        '''
        '''
        image_paths = sorted(
            glob.glob(os.path.join(self.input_folder, self.input_pattern)))

        if not image_paths:
            raise RuntimeError(
                'No input images found at {} matching {}'.format(
                    self.input_folder, self.input_pattern))

        yield RunBinarySegmentationModelPredictionTask(
            image_paths=image_paths,
            output_folder=self.output_folder,
            model_folder=self.model_folder,
            model_weights_fname=self.model_weights_fname,
            downsampling=self.downsampling)
