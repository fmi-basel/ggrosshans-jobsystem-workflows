import logging

def add_progress(task, increment):
    '''Updates progress in frontend through lgrunner.
    '''
    task.trigger_event('event.lgrunner.progress.notification', task,
                       'add_percentage', increment)


class LGRunnerLoggingMixin:
    '''
    Mixin for lgrunner logging interface.
    '''

    def log(self, level, msg):
        '''
        '''
        self.trigger_event('event.lgrunner.log.notification', self, level, msg)

    def log_error(self, msg):
        '''
        '''
        self.log(logging.ERROR, msg)

    def log_info(self, msg):
        '''
        '''
        self.log(logging.INFO, msg)

    def log_debug(self, msg):
        '''
        '''
        self.log(logging.DEBUG, msg)

    def log_warning(self, msg):
        '''
        '''
        self.log(logging.WARNING, msg)
