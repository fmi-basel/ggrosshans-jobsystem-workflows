'''Provides quantification workflows for image-based assay.

'''
import luigi
from ggjw.workflows.base import JobSystemWorkflow
from ggjw.tasks.quantification.knime_quantification import WormQuantificationTask


class WormQuantificationWorkflow(luigi.WrapperTask, JobSystemWorkflow):
    '''executes the worm quantification workflow on a given set of images
    and their corresponding segmentations.

    '''
    image_folder = luigi.Parameter()
    '''folder containing the images to be processed.

    '''
    segm_folder = luigi.Parameter()
    '''folder containing the segmentation corresponding to
    the images in image_folder.

    '''
    output_folder = luigi.Parameter()
    '''output folder into which the quantification results will be
    written.

    '''

    image_file_pattern = luigi.Parameter()
    '''fname pattern matching images of the channel that should
    be quantified. E.g. "*w1*" for all images with w1 in the filename.

    NOTE the underlying KNIME workflow seems to have problems with "_"
    in the file pattern. These should be avoided.

    '''

    {"default": "127"}
    threshold = luigi.FloatParameter(default=127.0)
    '''threshold to be applied on the segmentation probabilities. Higher
    values lead to more conservative segmentations. The threshold has
    to be within [0, 255].

    '''

    task_namespace = 'ggrosshans'
    resources = {'gpu': 0}

    def requires(self):
        '''launch the actual quantification task.
        '''
        yield WormQuantificationTask(
            image_folder=self.image_folder,
            segm_folder=self.segm_folder,
            output_folder=self.output_folder,
            image_file_pattern=self.image_file_pattern,
            threshold=self.threshold)
