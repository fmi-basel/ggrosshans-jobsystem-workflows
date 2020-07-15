import os
import subprocess
import luigi

from faim_luigi.tasks.knime import KnimeWrapperTaskBase
from faim_luigi.tasks.knime import format_workflow_arg
from faim_luigi.tasks.knime import format_workflow_variable_arg

from ggjw.tasks.logging import LGRunnerLoggingMixin

DEFAULT_WORKFLOW = os.path.join(os.path.dirname(__file__), 'res',
                                'worm_quantification_cnn.knwf')


class WormQuantificationTask(KnimeWrapperTaskBase, LGRunnerLoggingMixin):
    '''executes the worm quantification workflow in KNIME.

    NOTE the workflow needs to have all variables that are handed over
    in workflow_args defined as global flow variables!

    '''

    workflow_path = luigi.Parameter(default=DEFAULT_WORKFLOW)
    '''path to knime workflow.
    '''

    image_folder = luigi.Parameter()
    segm_folder = luigi.Parameter()
    output_folder = luigi.Parameter()

    image_file_pattern = luigi.Parameter()
    '''fname pattern matching images of the channel that should
    be quantified. E.g. "*w1*" for all images with w1 in the filename.
    '''
    @property
    def output_folder_images(self):
        return os.path.join(self.output_folder, 'kymographs')

    @property
    def output_path_csv(self):
        return os.path.join(self.output_folder, 'kymograph_stats.csv')

    @property
    def output_path_table(self):
        return os.path.join(self.output_folder, 'kymograph_stats.table')

    @property
    def workflow(self) -> str:
        return format_workflow_arg(self.workflow_path)

    @property
    def workflow_args(self) -> list:
        return super().workflow_args + [
            format_workflow_variable_arg(knime_var, var_value)
            for knime_var, var_value in [  # knime variable <-> value pairs
                ('image_folder', self.image_folder),
                ('image_pattern', self.image_file_pattern),  #
                ('segm_folder', self.segm_folder),
                ('output_folder_images', self.output_folder_images),
                ('output_path_csv', self.output_path_csv),
                ('output_path_table', self.output_path_table)
            ]
        ]

    def run(self):
        self.log_info('Starting knime workflow...')
        try:
            super().run()
        except subprocess.CalledProcessError as err:
            self.log_error('Knime workflow ({}) failed. Error log: {}'.format(
                self.workflow_path, err.stderr))
            raise
        self.log_info('Done.')

    def output(self):
        '''NOTE only check for successfully generated statistics
        and ignore the kymograph images for the time being.

        '''
        return [
            luigi.LocalTarget(path)
            for path in [self.output_path_csv, self.output_path_table]
        ]
