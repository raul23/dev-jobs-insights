import logging
import logging.config
import os
import sqlite3
import sys

import ipdb
import yaml

# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects/dev_jobs_insights/database"))
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
    import pickle

    with open(os.path.expanduser('~/data/dev_jobs_insights/scraped_job_data/20180915-044000-scraped_job_data/all_sessions-0-50.pkl'), 'rb') as f:
        data = pickle.load(f)

    with open(os.path.expanduser('~/data/dev_jobs_insights/scraped_job_data/20180915-044000-scraped_job_data/all_sessions-50-99.pkl'), 'rb') as f:
        data2 = pickle.load(f)

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import tables

    # Create tables
    engine = create_engine('sqlite:///test.db')
    # Base.metadata.create_all(engine)

    # Setup db session
    tables.Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()

    job_data_tablenames = [tables.Company.__tablename__, tables.JobPost.__tablename__]
    job_data_tablenames.extend([v.__tablename__ for v in tables.__dict__.values()
                                if hasattr(v, '__tablename__')
                                and v.__tablename__ !=job_data_tablenames])
    ipdb.set_trace()
    for k, v in data.items():
        job_data = v.data
        for attr_key, attr_value in job_data.__dict__.items():
            if hasattr(attr_value, '__tablename__'):
                cur_tablename = attr_value.__tablename__
                for tablename in job_data_tablenames:
                    # Add instance to table
                    session.add(job_data.company)

        session.commit()

    # Read yaml configuration file
    try:
        config_dict = read_yaml_config(CONFIG_FILEPATH)
    except OSError as e:
        logger.error(e.__str__())
        logger.error("Configuration file '{}' couldn't be read. "
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
        data_filepath = os.path.expanduser(config_dict['scraped_job_data_filepath'])
        scraped_data = gu.load_json_with_codecs(data_filepath)
    except FileNotFoundError as e:
        logger.error(e.__str__())
        logger.error('Scraped data could not be loaded. Program will exit.')
        sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt as e:
        pass
