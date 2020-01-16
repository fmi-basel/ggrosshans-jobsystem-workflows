'''provides utility for checking for interrupts from the lgrunner.
'''


class StopSignalException(Exception):
    pass


class StoppableTaskMixin:

    accepts_messages = True

    def raise_if_interrupt_signal(self):
        '''checks if the given tasks has received an interrupt signal.

        Parameters
        ----------
        task : luigi.Task
            current task.
        '''
        msg = None
        if (self.scheduler_messages is not None
                and not self.scheduler_messages.empty()):
            msg = self.scheduler_messages.get().content
        if msg == 'SIGTERM':
            raise StopSignalException(
                '%s terminated by scheduler message "SIGTERM"!' % self.task_id)
