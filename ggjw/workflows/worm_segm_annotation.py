'''Provides workflows for data preparation for annotation for
the worm segmentation training workflow.

'''
import luigi

from ggjw.workflows.base import JobSystemWorkflow

from ggjw.tasks.segmentation.prepare_for_annotation import PrepareRandomlySelectedImagesForAnnotation
from ggjw.tasks.segmentation.prepare_for_annotation import PrepareManuallySelectedImagesForAnnotation

# NOTE this defines the workflows as they are picked up by the MJS
# frontend.  Unfortunately, it does not discover inherited parameters,
# such that we have to replicate the paramteer definition here instead
# of just using inheritance.


class PrepareRandomlySelectedImagesForAnnotationWorkflow(
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
    '''Output folder to write projections into.
    '''

    num_samples = luigi.IntParameter()
    '''Number of samples to draw.
    '''

    seed = luigi.IntParameter(default=13)
    '''Seed for random sampling.
    '''

    task_namespace = 'ggrosshans'

    def requires(self):
        '''launch the actual worker tasks.
        '''
        yield PrepareRandomlySelectedImagesForAnnotation(
            input_folder=self.input_folder,
            output_folder=self.output_folder,
            file_pattern=self.file_pattern,
            num_samples=self.num_samples,
            seed=self.seed)


class PrepareManuallySelectedImagesForAnnotationWorkflow(
        luigi.WrapperTask, JobSystemWorkflow):
    '''Prepare images for annotation based on a manually generated
    list of positions and timepoints.
    '''

    input_folder = luigi.Parameter()
    '''Input folder to collect from.
    '''

    file_pattern = luigi.Parameter()
    '''Filename pattern matching files to be included.
    Allowed wildcards are *, ?, [seq] and [!seq] (see fnmatch).

    Note that this pattern should *not* contain position, timepoint or
    file extension as it will be combined with subfolder, position and
    timepoint from the given input_file.

    '''

    output_folder = luigi.Parameter()
    '''Output folder to write projections into.
    '''

    input_file = luigi.Parameter()
    '''Manually generated collections of images to prepare [.csv].

    Must contain the following columns:

    folder, position, timepoint

    the files are expected at the following location:

    <input_folder>/<folder>/<file_pattern><delimiter><position><delimiter><timepoint><file_extension>
    '''

    task_namespace = 'ggrosshans'

    def requires(self):
        '''launch the actual worker tasks.
        '''
        yield PrepareManuallySelectedImagesForAnnotation(
            input_folder=self.input_folder,
            output_folder=self.output_folder,
            input_file=self.input_file,
            file_pattern=self.file_pattern)
