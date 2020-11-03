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

    The input is expected to be 3-dim. The first axis is then projected
    and the output is a 2-dim segmentation.

    See model_version for information on the output probabilities.
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
    '''Choose the segmentation model version.

    It is strongly recommended to use the most recent version.

    Model versions:
    ---
    v2 => Current release. Trained on data from v1 extended with
          additional early timepoints.
    v1 => Trained on data from Yannick, Milou and Lucas.
          (previously ExperimentalWormSegmentationFromBrightFieldWorkflow).
    v0 => Trained only on Yannick's GFP-based segmentation.

    Output probabilities:
    ---
    The outputs are probabilities from the segmentation model scaled
    to the range of [0, 255], where 255 corresponds to a probability
    of 1. You can obtain a hard segmentation, e.g. by applying a
    simple threshold.

    The different models may be very differently calibrated, depending
    on the training data they have seen. This means that the range of
    probabilities they output may differ, e.g. being skewed or tending
    to be more/less extreme. Therefore, it is recommended to check if
    the threshold in the subsequent analysis is appropriate and adjust
    if necessary whenever you used a different segmentation model (see
    threshold parameter of the WormQuantificationWorkflow). A good
    initial guess is to use a threshold of P=0.5, which would
    correspond to a scaled threshold value of 127.

    The resulting segmentation files contain a metadata entry on the
    model_version that was used to generate them.

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
