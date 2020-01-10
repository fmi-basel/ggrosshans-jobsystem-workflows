import logging


class LGRunnerLoggingMixin:
    '''
    Mixin for lgrunner logging interface.
    '''
    # TODO Consider moving this to faim-luigi?

    def log(self, level, msg):
        '''
        '''
        self.trigger_event(
            'event.lgrunner.log.notification',
            self,
            level,
            msg)

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
