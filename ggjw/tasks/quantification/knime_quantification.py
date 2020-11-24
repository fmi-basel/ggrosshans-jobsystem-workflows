import os
import re
import logging
import subprocess
from glob import glob

import luigi

from faim_luigi.tasks.knime import KnimeWrapperTaskBase
from faim_luigi.tasks.knime import format_workflow_arg
from faim_luigi.tasks.knime import format_workflow_variable_arg

from ggjw.tasks.logging import LGRunnerLoggingMixin
from ggjw.tasks.lgrunner.stop import StoppableTaskMixin, StopSignalException

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

    threshold = luigi.FloatParameter(default=127.0)
    '''threshold to be applied on the segmentation probabilities. Higher
    values lead to more conservative segmentations. The threshold has
    to be within [0, 255].

    '''

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
        ] + [
            format_workflow_variable_arg('threshold', self.threshold, 'double')
        ]

    def _check_input_folders(self):
        '''check if input folders exist.
        '''
        failed = False

        # check if input folders exist
        for param_name, param_value in ((param, getattr(
                self, param)) for param in ['image_folder', 'segm_folder']):
            if not os.path.exists(param_value):
                self.log_error('Given {}={} does not exist!'.format(
                    param_name, param_value))
                failed = True

        if failed:
            raise FileNotFoundError(
                'Given image_folder and/or segm_folder do not exist!')

    def _check_matches(self):
        '''check if any image and segmentation files match.
        '''
        def extract_pos_time(path):
            match = re.search(r'_s(\d+)_t(\d+)\..+', os.path.basename(path))
            if match:
                return match.groups()
            return None

        def count_corresponding(image_paths, segm_paths, extract_pos_time_fn):
            '''matches image and segmentation file paths based on the position and
            timepoint extracted from the filename by
            extract_pos_time_fn

            '''
            def is_not_none(val):
                return val is not None

            segm_candidates = set(
                filter(is_not_none, map(extract_pos_time_fn, segm_paths)))
            image_candidates = set(
                filter(is_not_none, map(extract_pos_time_fn, image_paths)))

            return len(image_candidates.intersection(segm_candidates))

        # check if some image files match
        image_matches = glob(
            os.path.join(self.image_folder, self.image_file_pattern))
        if not image_matches:
            raise FileNotFoundError(
                'No files in image_folder match the given image_file_pattern={}'
                .format(self.image_file_pattern))

        self.log_info('Found {} files matching image_file_pattern'.format(
            len(image_matches)))

        # check if any corresponding segmentations exist
        matching_files = count_corresponding(
            image_matches, glob(os.path.join(self.segm_folder, '*')),
            extract_pos_time)

        if matching_files <= 0:
            raise FileNotFoundError(
                'Could not find any corresponding image and segmentation pairs!'
            )

        self.log_info(
            'Found {} image-segmentation file pairs'.format(matching_files))

    def _check_threshold(self):
        '''make sure the threshold makes sense.
        '''
        if self.threshold < 0 or self.threshold > 255:
            raise ValueError(
                'threshold is {} but has to be in [0, 255]'.format(
                    self.threshold))

    def check_input_parameters(self):
        '''verify if given input folders exists and contain files matching the
        given pattern.

        NOTE this is somewhat redundant to logic in the knime
        workflow, but it helps to detect wrong parametrization early
        and give precise feedback to the user.

        '''
        self._check_input_folders()
        self._check_matches()
        self._check_threshold()

    def run(self):
        '''runs the knime workflow in a separate process with Popen. This
        enables interrupting the task through the job system.

        '''
        try:
            self.check_input_parameters()
        except Exception as err:
            self.log_error('Invalid input parameters: {}'.format(err))
            raise

        cmd = self.compose_call()
        logger = logging.getLogger('luigi-interface')

        with open(self.output_path_log, 'w') as logfile, \
             subprocess.Popen(args=cmd,
                              stdout=logfile,
                              stderr=logfile) as process:

            self.log_info('Starting knime workflow...')

            while True:
                try:
                    self.raise_if_interrupt_signal()
                except StopSignalException:
                    logger.debug('Sending kill signal to knime process')
                    process.kill()
                    logger.debug('Kill signal sent')
                    raise
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
