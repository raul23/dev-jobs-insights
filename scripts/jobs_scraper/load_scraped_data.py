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

    # Open connection to db
    conn = gu.connect_db(os.path.expanduser(config_dict['db_filepath']))

    # Init list of SQL queries. One list per SQL tables
    job_posts = []
    hiring_company = []
    experience_level = []
    industry = []

    ipdb.set_trace()

    # Get the column names for each tables
    # From the `job_posts` table
    # From the `hiring_company` table
    # From the `experience_level` table
    # From the `role` table
    # From the `industry` table
    # From the `skills` table
    # From the `job_benefits` table
    # From the `job_salary` table
    # From the `location` table

    job_posts_cols = ['title', 'url', 'company_name', 'job_post_description', 'job_post_notice', 'employment_type',
                      'equity', 'high_response_rate', 'remote', 'relocation', 'visa', 'cached_webpage_path',
                      'date_posted', 'valid_through', 'webpage_accessed']
    hiring_company_cols = ['company_name', 'company_site_url', 'company_description', 'company_type', 'company_size']
    experience_level_cols = ['level']
    role_cols = ['role']
    industry_cols = ['name']
    skills_cols = ['skill']
    job_benefits_cols = ['name']
    job_salary_cols = ['min_salary', 'max_salary', 'currency', 'currency_conversion_time']
    location_cols = ['city', 'region', 'country']

    count = 1
    for job_id, job_data in scraped_data.items():
        logger.info("#{} Processing {}".format(count, job_id))
        count += 1

        ipdb.set_trace()

        # Get all the required values for populating the `job_posts` table
        job_post_values = [job_id]
        for col in job_posts_cols:
            value = job_data[col]
            job_post_values.append(value)

        ipdb.set_trace()

        # Get all the required values for populating the `hiring_company` table
        hiring_company_values = [job_id]
        for col in hiring_company:
            value = job_data[col]
            hiring_company_values.append(value)

    # Execute the bulk of SQL queries
    with conn:
        pass


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt as e:
        pass
