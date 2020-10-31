'''Provides workflows for segmentation of worms.

'''
import os

import luigi

from ggjw.models import MODEL_BASE_FOLDER
from ggjw.tasks.segmentation.fcn_task import RunBinarySegmentationModelPredictionTaskV0
from ggjw.tasks.segmentation.fcn_task import RunBinarySegmentationModelPredictionTask
from ggjw.workflows.base import JobSystemWorkflow

MODEL_FOLDER_FOR_VERSION = {
    version: os.path.join(MODEL_BASE_FOLDER, 'segmentation', 'worms_from_bf',
                          version)
    for version in ['v0', 'v1', 'v2']  # Currently supported versions
}


class WormSegmentationFromBrightFieldWorkflow(luigi.WrapperTask,
                                              JobSystemWorkflow):
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

    {"default": "v2", "choices": ["v0", "v1", "v2"]}
    model_version = luigi.ChoiceParameter(choices=["v0", "v1", "v2"],
                                          default="v2")
    '''Choose the segmentation model version. It is strongly recommended
    to use the most recent version.

    '''

    task_namespace = 'ggrosshans'
    resources = {'gpu': 1}

    @property
    def model_folder(self):
        '''path of saved model.
        '''
        return MODEL_FOLDER_FOR_VERSION[self.model_version]

    def requires(self):
        '''launch the actual segmentation task.
        '''
        if self.model_version == 'v0':
            model_weights_fname = 'model_latest.h5'
            yield RunBinarySegmentationModelPredictionTaskV0(
                input_folder=self.input_folder,
                file_pattern=self.file_pattern,
                output_folder=self.output_folder,
                model_folder=self.model_folder,
                model_weights_fname=model_weights_fname)
        else:
            yield RunBinarySegmentationModelPredictionTask(
                input_folder=self.input_folder,
                file_pattern=self.file_pattern,
                output_folder=self.output_folder,
                model_folder=self.model_folder)
