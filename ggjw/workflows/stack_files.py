import luigi
from ggjw.workflows.base import JobSystemWorkflow
from ggjw.tasks.data_management.stack_files import StackFilesTask


class StackFilesWorkflow(luigi.WrapperTask, JobSystemWorkflow):
    """
    A task that stacks 3D (X, Y, Z) visiview images from multiple timepoints
    and stage positions into a 5D imagej tif (T, Z, C, X, Y).
    """

    image_folder = luigi.Parameter()
    """
    Folder containing the images to be processed.
    """

    image_file_pattern = luigi.Parameter()
    """
    Filename pattern matching images of the channel that should
    be quantified. E.g. "*w1*" for all images with w1 in the filename.
    """

    {"default": "st", "choices": ["st", "ts"]}
    data_format = luigi.ChoiceParameter(choices=["st", "ts"], default="st")
    """
    Visiview automatically writes the data out with 's_' (referring to the stage position)
    and 't_' (referring to the time point) in the file-name. However, sometimes the order of 's_'
    and 't_' is inverted. Here we provide information on this order.
    """

    output_folder = luigi.Parameter()
    """
    Output folder into which the quantification results will be
    written.
    """

    task_namespace = 'ggrosshans'
    resources = {'gpu': 0}

    def requires(self):
        """
        Launch the actual stacking task.
        """
        yield StackFilesTask(

            image_folder=self.image_folder,
            image_file_pattern=self.image_file_pattern,
            data_format=self.data_format,
            output_folder=self.output_folder)
