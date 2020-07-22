'''image compression base task.

'''
import abc
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

import luigi
from luigi.util import requires

from faim_luigi.tasks.collectors import ImageCollectorTask

from ggjw.tasks.logging import LGRunnerLoggingMixin, add_progress
from ggjw.tasks.lgrunner.stop import StoppableTaskMixin


@requires(ImageCollectorTask)
class BaseCompressionTask(luigi.Task, LGRunnerLoggingMixin,
                          StoppableTaskMixin):
    '''Base task for image compression.
    '''

    accepts_messages = True

    num_threads = luigi.IntParameter(default=8)

    @abc.abstractmethod
    def get_target(self, input_handle):
        '''returns a luigi target for a given input_target.

        '''

    @abc.abstractmethod
    def convert(self, input_target, output_target):
        '''converts the image at input_target and writes it to output_target.

        '''

    def run(self):
        '''calls convert method on each input-output pair.

        '''
        iterable = self._get_iterable()
        self._report_initial(iterable)
        self.log_info('Starting to convert {} images'.format(len(iterable)))
        error_count = 0

        with ThreadPoolExecutor(max_workers=self.num_threads) as pool:

            futures = [
                pool.submit(self.convert, input_handle, target)
                for input_handle, target in iterable
            ]

            for future in as_completed(futures):

                # NOTE we assume here that the individual steps are
                # fast enough such that we dont need to have an extra
                # waiting loop to check for interrupts.
                self.raise_if_interrupt_signal()

                try:
                    future.result()
                except Exception as err:
                    self.log_error(
                        'Failed to convert an item. Error: {}'.format(err))
                    error_count += 1
                finally:
                    add_progress(self, self._fraction)

        if error_count >= 1:
            raise RuntimeError('Encountered {} errors!'.format(error_count))
        self.log_info('Done.')

    def _get_iterable(self) -> list:
        '''returns iterable over input and target pairs.

        '''
        iterable = [
            (input_handle, target)
            for input_handle, target in ((input_handle,
                                          self.get_target(input_handle))
                                         for input_handle in self.input())
            if not target.exists()
        ]

        return iterable

    @property
    def _fraction(self):
        return 100. / len(self.input())

    def _report_initial(self, iterable):
        '''reports number of inputs to process and updates
        the progressbar.
        '''
        num_inputs = len(self.input())
        num_done = len(self.input()) - len(iterable)
        self.log_info(
            'Found {} input images. {} are already processed.'.format(
                num_inputs, num_done))

        add_progress(self, num_done * self._fraction)

    def output(self):
        '''generates list of all output targets.
        '''
        if not self.input():
            raise ValueError('No input images provided!')

        return [self.get_target(input_handle) for input_handle in self.input()]
