import argparse
import glob
import logging
import logging.config
import os
import sys
# Third-party modules
import ipdb
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
import yaml
# Own modules
# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser(
    "~/PycharmProjects/github_projects"))
sys.path.insert(0, os.path.expanduser(
    "~/PycharmProjects/github_projects/dev_jobs_insights/database"))
from tables import Base
from utility import genutil as gu


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

color_levels_std = {
    'debug':        "\033[36m{}\033[0m",        # Blue
    'info':         "\033[32m{}\033[0m",        # Green
    'warning':      "\033[33m{}\033[0m",        # Yellow
    'error':        "\033[31m{}\033[0m",        # Red
    'exception':    "\033[31m{}\033[0m",        # Red
    'critical':     "\033[7;31;31m{}\033[0m"    # Red highlighted
}
# Colors for Pycharm terminal:
color_levels_pycharm = {
    'debug':        "\033[34m{}\033[0m",        # Blue
    'info':         "\033[36m{}\033[0m",        # Blue aqua
    'warning':      "\033[32m{}\033[0m",        # Yellow
    'error':        "\033[31m{}\033[0m",        # Red
    'exception':    "\033[31m{}\033[0m",        # Red
    'critical':     "\033[7;31;31m{}\033[0m"    # Red highlighted
}
color_levels = color_levels_std
log_levels = ['debug', 'info', 'warning', 'error', 'exception', 'critical']


def get_error_msg(exc):
    error_msg = '[{}] {}'.format(exc.__class__.__name__, exc.__str__())
    return error_msg


# TODO: these functions will be in gentuil (might replace those already there)
def load_yaml(f):
    try:
        return yaml.load(f)
    except yaml.YAMLError as e:
        raise yaml.YAMLError(e)


# By default (if `logger` is None), the file and console loggers are used
# By default, the console logger's messages are colored (`use_color` is True)
def log(msg, level, logger=None, use_color=True):
    assert level in log_levels, "Log level '' not valid".format(level)
    if logger is None:
        clogger_method = clogger.__getattribute__(level)
        if use_color:
            clogger_method(set_color(msg, level))
        else:
            clogger_method(msg, level)
        flogger_method = flogger.__getattribute__(level)
        flogger_method(msg)
    else:
        logger_method = logger.__getattribute__(level)
        if use_color:
            logger_method(set_color(msg, level))
        else:
            logger_method(msg, level)


def read_yaml_config(config_path):
    try:
        with open(config_path, 'r') as f:
            return load_yaml(f)
    except (OSError, yaml.YAMLError) as e:
        raise OSError(e)


# ref.: https://stackoverflow.com/a/45924203
def set_color(msg, level=None):
    if level is None:
        return color_levels['info'].format(msg)
    else:
        return color_levels[level].format(msg)


def setup_logging(config_path):
    # Read yaml configuration file
    try:
        config_dict = read_yaml_config(config_path)
    except OSError as e:
        raise OSError(e)
    # Update the logging config dict with new values from `config_dict`
    try:
        logging.config.dictConfig(config_dict)
    except ValueError as e:
        raise ValueError(e)


def main():
    # Setup argument parser
    parser = argparse.ArgumentParser(
        description="Load scraped job data from pickle files into a database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "-e", "--error_log_level", choices=['error', 'exception'],
        default="error",
        help="The logging level used for logging messages when an exception is "
             "caught.")
    parser.add_argument(
        "-l", "--log_config", default="logging_config.yaml",
        help="Filepath to the YAML logging configuration file.")
    parser.add_argument(
        "-m", "--main_config", default="config.yaml",
        help="Filepath to the YAML main configuration file.")
    parser.add_argument(
        "-p", "--pycharm_colors",
        action='store_true',
        default=False,
        help="Use colors for the logging messages as specified for the Pycharm "
             "Terminal. By default, we use colors for the logging messages as "
             "defined for the standard Unix Terminal.")
    # Process command-line arguments
    args = parser.parse_args()

    # Setup root logger without the logging configuration
    rlogger = logging.getLogger()
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    ch.setFormatter(formatter)
    rlogger.addHandler(ch)
    rlogger.setLevel(logging.DEBUG)

    if args.pycharm_colors:
        global color_levels
        color_levels = color_levels_pycharm
        log("The colors of the logging messages are those used for the Pycharm "
            "Terminal", level='info', logger=rlogger)

    # Setup loggers from YAML logging configuration file
    try:
        log(
            "Setting up logging from the YAML configuration file '{}'".format(
                args.log_config),
            level='info', logger=rlogger)
        setup_logging(args.log_config)
    except (OSError, ValueError, KeyError) as e:
        # TODO: change all 'error' levels to 'exception' once you are able to
        # write the traceback with one line, see https://bit.ly/2DkH63E
        log(get_error_msg(e), level=args.error_log_level, logger=rlogger)
        log("Logging could not be setup. Program will exit.",
            level='error', logger=rlogger)
        sys.exit(1)
    else:
        log("Logging setup completed", 'debug')

    # Read YAML configuration file
    try:
        log("Loading the YAML configuration file '{}'".format(args.main_config),
            level='info')
        config_dict = read_yaml_config(args.main_config)
    except OSError as e:
        log(get_error_msg(e), level=args.error_log_level)
        log("Configuration file '{}' couldn't be read. Program will "
            "exit.".format(args.main_config),
            level='error')
        sys.exit(1)
    else:
        log("Successfully loaded the YAML configuration file", 'debug')

    # SQLAlchemy database setup
    log("Database setup", 'info')
    # Create tables
    db_url = config_dict['db_url']
    db_url['database'] = os.path.expanduser(db_url['database'])
    engine = create_engine(URL(**db_url))
    Base.metadata.create_all(engine)

    # Setup database session
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    db_session = DBSession()

    # Load the scraped job data as pickle files
    # TODO: add a progress bar with the tqdm library which is part of anaconda
    # ref.: https://stackoverflow.com/a/29703127
    log("Loading the scraped job data as pickle files", 'info')
    data_dirpath = os.path.expanduser(config_dict['scraped_job_data_dirpath'])
    list_job_data_filepaths = glob.glob(os.path.join(data_dirpath, "*.pkl"))
    log(
        "There are {} pickle files in '../{}/'".format(
            len(list_job_data_filepaths), os.path.basename(data_dirpath)),
        level='info')
    for i, job_data_filepath in enumerate(list_job_data_filepaths, start=1):
        try:
            log(
                "#{} Loading the pickle file '{}'".format(
                    i, os.path.basename(job_data_filepath)),
                level='info')
            scraped_job_data = gu.load_pickle(job_data_filepath+'s')
        except FileNotFoundError as e:
            log(get_error_msg(e), level=args.error_log_level)
            log(
                "Scraped job data from '{}' could not be loaded. Program will "
                "exit.".format(os.path.basename(job_data_filepath)),
                level='error')
            sys.exit(1)
        else:
            log(
                "Finished loading the scraped job data from '{}'".format(
                    job_data_filepath),
                level='debug')

        # Load the scraped job data into the database
        log(
            "Loading the scraped job data '{}' into the database".format(
                os.path.basename(job_data_filepath)),
            level='info')
        for j, (job_post_id, scraping_session) in \
                enumerate(scraped_job_data.items(), start=1):
            try:
                log(
                    "#{} Adding job data for job_post_id={}".format(
                        j, job_post_id),
                    level=args.error_log_level)
                db_session.add(scraping_session.data.company)
                db_session.commit()
            except IntegrityError as e:
                # Possible cause #1: UNIQUE constraint failed
                # Example: adding a `job_post` with a `job_posts.id` already taken
                # Possible cause #2: NOT NULL constraint failed
                # Example: adding a `job_post` without an URL which is mandatory as
                # specified in the schema
                log(get_error_msg(e), 'error')
                db_session.rollback()
            else:
                log(
                    "Successfully added job data for job_post_id={}".format(
                        job_post_id),
                    level='debug')


if __name__ == '__main__':
    try:
        main()
    except (KeyboardInterrupt, KeyError) as e:
        pass
