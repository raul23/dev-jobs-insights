import logging
import logging.config
import os
import sqlite3
import sys

import ipdb
import yaml

# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
from utility import genutil as gu


CONFIG_FILEPATH = "config.yaml"


# Get the logger
# TODO: can this be done in a genutil.function (i.e. outside of this file)?
if __name__ == '__main__':
    # When run as a script
    logger = logging.getLogger('{}.{}'.format(os.path.basename(os.getcwd()), os.path.splitext(__file__)[0]))
else:
    # When imported as a module
    # TODO: test this part when imported as a module
    logger = logging.getLogger('{}.{}'.format(os.path.basename(os.path.dirname(__file__)), __name__))


# TODO: these functions will be in gentuil (might replace those already there)


def load_yaml(f):
    try:
        return yaml.load(f)
    except yaml.YAMLError as e:
        raise yaml.YAMLError(e)


def read_yaml_config(config_path):
    try:
        with open(config_path, 'r') as f:
            return load_yaml(f)
    except (OSError, yaml.YAMLError) as e:
        raise OSError(e)


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
    ipdb.set_trace()
    # Read yaml configuration file
    try:
        config_dict = read_yaml_config(CONFIG_FILEPATH)
    except OSError as e:
        logger.error(e.__str__())
        logger.error("Configuration file '{}' could be read. "
                     "Program will exit.".format(CONFIG_FILEPATH))
        sys.exit(1)

    # Setup logging
    try:
        setup_logging(config_dict['logging_filepath'])
    except (OSError, ValueError) as e:
        logger.error(e.__str__())
        logger.error('Logging could not be setup. Program will exit.')
        sys.exit(1)

    # Load the scraped data
    try:
        scraped_data = gu.load_json_with_codecs(os.path.expanduser(config_dict['scraped_job_data_filepath']))
    except FileNotFoundError as e:
        logger.error(e.__str__())
        logger.error('Scraped data could not be loaded. Program will exit.')
        sys.exit(1)

    # Init list of SQL queries
    job_posts = []
    job_perks = []
    job_salary = []
    job_overview = []

    ipdb.set_trace()

    count = 1
    for job_id, job_data in scraped_data.items():
        logger.info("#{} Processing {}".format(count, job_id))
        count += 1


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt as e:
        pass
