import os
import subprocess
import luigi

from faim_luigi.tasks.knime import KnimeWrapperTaskBase
from faim_luigi.tasks.knime import format_workflow_arg
from faim_luigi.tasks.knime import format_workflow_variable_arg

from ggjw.tasks.logging import LGRunnerLoggingMixin
from ggjw.tasks.lgrunner.stop import StoppableTaskMixin

DEFAULT_WORKFLOW = os.path.join(os.path.dirname(__file__), 'res',
                                'worm_quantification_cnn.knwf')


class WormQuantificationTask(KnimeWrapperTaskBase, LGRunnerLoggingMixin,
                             StoppableTaskMixin):
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

    accepts_messages = True

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
    def output_path_log(self):
        os.makedirs(self.output_folder, exist_ok=True)
        return os.path.join(self.output_folder, 'knime-workflow.log')

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
        '''runs the knime workflow in a separate process with Popen. This
        enables interrupting the task through the job system.

        '''
        cmd = self.compose_call()

        with open(self.output_path_log, 'w') as logfile, \
             subprocess.Popen(args=cmd,
                              stdout=logfile,
                              stderr=logfile) as process:

            self.log_info('Starting knime workflow...')

            while True:
                self.raise_if_interrupt_signal()

                try:
                    retcode = process.wait(timeout=1.0)
                    if retcode:
                        self.log_error(
                            '''Knime workflow failed with error code {}.
                            Details can be found in the logfile at {}'''.
                            format(retcode, self.output_path_log))
                        raise subprocess.CalledProcessError(retcode, cmd)
                    break
                except subprocess.TimeoutExpired:
                    pass

        self.log_info('Done.')

    def output(self):
        '''NOTE only check for successfully generated statistics
        and ignore the kymograph images for the time being.

        '''
        return [
            luigi.LocalTarget(path)
            for path in [self.output_path_csv, self.output_path_table]
        ]
