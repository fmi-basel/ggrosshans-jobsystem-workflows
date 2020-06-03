'''Provides workflows for segmentation of worms.

'''
import os

import luigi

from ggjw.models import MODEL_BASE_FOLDER
from ggjw.tasks.segmentation.fcn_task import RunBinarySegmentationModelPredictionTask
from ggjw.workflows.base import JobSystemWorkflow

# basic lookup of model folder. Should be delegated in the future.


class WormSegmentationFromBrightFieldWorkflow(luigi.WrapperTask, JobSystemWorkflow):
    '''worm segmentation with a CNN from brightfield image stacks.

    NOTE input is expected to be 3-dim, the first axis is then projected
    and the output is a 2-dim segmentation.

    '''
    input_folder = luigi.Parameter()
    '''base folder containing the images to be processed.

    '''

    file_pattern = luigi.Parameter()
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

    model_folder = os.path.join(MODEL_BASE_FOLDER, 'segmentation',
                                'worms_from_bf', 'v0')
    '''path of saved model.

    '''
    model_weights_fname = 'model_latest.h5'
    '''file name of model weights.

    '''

    task_namespace = 'ggrosshans'
    resources = {'gpu': 1}

    def requires(self):
        '''launch the actual segmentation task.
        '''
        yield RunBinarySegmentationModelPredictionTask(
            input_folder=self.input_folder,
            file_pattern=self.file_pattern,
            output_folder=self.output_folder,
            model_folder=self.model_folder,
            model_weights_fname=self.model_weights_fname)
