'''Provides quantification workflows for image-based assay.

'''
import luigi
from ggjw.workflows.base import JobSystemWorkflow
from ggjw.tasks.data_management.delete_files import DeleteFilesTask


class DeleteFilesWorkflow(luigi.WrapperTask, JobSystemWorkflow):
    '''executes the worm quantification workflow on a given set of images
    and their corresponding segmentations.

    '''
    image_folder = luigi.Parameter()
    '''folder containing the images to be processed.

    '''
    data_format = luigi.Parameter()
    '''folder containing the segmentation corresponding to
    the images in image_folder.

    '''
    csv_document = luigi.Parameter()
    '''folder and csv file that tells which worms and which 
    timepoints should be deleted. Structure of the file should 
    have headings: "Position", "Quality", "Hatch", "Escape"

    '''

    image_file_pattern = luigi.Parameter()
    '''fname pattern matching images of the channel that should
    be quantified. E.g. "*w1*" for all images with w1 in the filename.

    '''



    task_namespace = 'ggrosshans'
    resources = {'gpu': 0}

    def requires(self):
        '''launch the actual quantification task.
        '''
        yield DeleteFilesTask(
            image_folder=self.image_folder,
            data_format=self.data_format,
            csv_document=self.csv_document,
            image_file_pattern=self.image_file_pattern)
