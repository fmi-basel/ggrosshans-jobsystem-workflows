'''Provides workflows for data preparation for annotation for
the worm segmentation training workflow.

'''
import luigi

from ggjw.workflows.base import JobSystemWorkflow

from ggjw.tasks.compression.compression_simple import StkToCompressedTifTask

# NOTE this defines the workflows as they are picked up by the MJS
# frontend.  Unfortunately, it does not discover inherited parameters,
# such that we have to replicate the parameter definition here instead
# of just using inheritance.


class StkToTifImageCompressionWorkflow(
        luigi.WrapperTask, JobSystemWorkflow):
    '''Prepare images for annotation by random sampling from
    the given experiment.
    '''

    input_folder = luigi.Parameter()
    '''Input folder to collect from.
    '''

    file_pattern = luigi.Parameter()
    '''Filename pattern matching files to be included.
    Allowed wildcards are *, ?, [seq] and [!seq] (see fnmatch)
    '''

    output_folder = luigi.Parameter()
    '''Output folder to write compressed stacks.
    '''

    task_namespace = 'ggrosshans'

    def requires(self):
        '''launch the actual worker tasks.
        '''
        yield StkToCompressedTifTask(
            input_folder=self.input_folder,
            output_folder=self.output_folder,
            file_pattern=self.file_pattern)
