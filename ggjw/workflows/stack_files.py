'''Provides quantification workflows for image-based assay.

'''
import luigi
from ggjw.workflows.base import JobSystemWorkflow
from ggjw.tasks.data_management.stack_files import StackFilesTask


class StackFilesWorkflow(luigi.WrapperTask, JobSystemWorkflow):
    '''executes the worm quantification workflow on a given set of images
    and their corresponding segmentations.

    '''
    image_folder = luigi.Parameter()
    '''folder containing the images to be processed.

    '''

    image_file_pattern = luigi.Parameter()
    '''fname pattern matching images of the channel that should
    be quantified. E.g. "*w1*" for all images with w1 in the filename.

    '''

    {"default": "st","choises":["st","ts"]}
    data_format = luigi.ChoiceParameter(choices=["st","ts"],default="st")
    '''order of time and position in files: 's_t_' or 't_s_'.

    '''
    output_folder = luigi.Parameter()
    '''output folder into which the quantification results will be
    written.

    '''

    task_namespace = 'ggrosshans'
    resources = {'gpu': 0}

    def requires(self):
        '''launch the actual quantification task.
        '''
        yield StackFilesTask(

            image_folder=self.image_folder,
            image_file_pattern=self.image_file_pattern,
            data_format=self.data_format,
            output_folder=self.output_folder)
