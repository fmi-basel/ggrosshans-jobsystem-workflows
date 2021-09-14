import luigi
from ggjw.workflows.base import JobSystemWorkflow
from ggjw.tasks.data_management.delete_files import DeleteFilesTask


class DeleteFilesWorkflow(luigi.WrapperTask, JobSystemWorkflow):
    """
    Executes the delete files workflow that deletes the images of worms which
    have not hatched or have escaped. The deletion is based on a provided
    'goodworms.csv' files, which indicates for each worm when it hatches and
    when it escapes.
    """
    image_folder = luigi.Parameter()
    """
    Folder containing the images to be processed.
    """

    {"default": "st", "choices": ["st", "ts"]}
    data_format = luigi.ChoiceParameter(choices=["st", "ts"], default="st")
    """
    Visiview automatically writes the data out with 's_' (referring to the stage position)
    and 't_' (referring to the time point) in the file-name. However, sometimes the order of 's_'
    and 't_' is inverted. Here we provide information on this order.
    """

    csv_document = luigi.Parameter()
    """
    Folder and csv file that tells which worms and which 
    timepoints should be deleted. Structure of the file should 
    have headings: "Position", "Quality", "Hatch", "Escape"
    """

    image_file_pattern = luigi.Parameter(default="*.tif")
    """
    File name pattern of the files to be considered for deletion.
    """

    task_namespace = 'ggrosshans'
    resources = {'gpu': 0}

    def requires(self):
        """
        Launch the actual deleting file task.
        """
        yield DeleteFilesTask(
            image_folder=self.image_folder,
            data_format=self.data_format,
            csv_document=self.csv_document,
            image_file_pattern=self.image_file_pattern)
