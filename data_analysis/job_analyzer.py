import logging
import os


# Get the loggers
# TODO: can this be done in a genutil.function (i.e. outside of this file)?
if __name__ == '__main__':
    # When run as a script
    flogger = logging.getLogger('{}.{}.file'.format(
        os.path.basename(os.getcwd()), os.path.splitext(__file__)[0]))
    clogger = logging.getLogger('{}.{}.console'.format(
        os.path.basename(os.getcwd()), os.path.splitext(__file__)[0]))
else:
    # When imported as a module
    # TODO: test this part when imported as a module
    flogger = logging.getLogger('{}.{}.file'.format(
        os.path.basename(os.path.dirname(__file__)), __name__))
    clogger = logging.getLogger('{}.{}.console'.format(
        os.path.basename(os.path.dirname(__file__)), __name__))


class JobDataAnalyzer:
    def __init__(self, config_path):
        pass

    def run_analysis(self):
        pass
