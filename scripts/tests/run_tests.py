import argparse
import os
import sys
# Third-party modules
import ipdb
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
# Own modules
from industries import run_tests_on_industries
from job_benefits import run_tests_on_job_benefits
from job_locations import run_tests_on_job_locations
from job_salaries import run_tests_on_job_salaries
from roles import run_tests_on_roles
from skills import run_tests_on_skills
from tables import Base
from utilities.genutils import read_yaml_config
from utilities.script_boilerplate import ScriptBoilerplate

logger = None
db_session = None


def get_db_session(main_cfg):
    # SQLAlchemy database setup
    db_url = main_cfg['db_url']
    db_url['database'] = os.path.expanduser(db_url['database'])
    logger.info("Database setup of {}".format(db_url['database']))
    engine = create_engine(URL(**db_url))
    Base.metadata.bind = engine
    # Setup database session
    DBSession = sessionmaker(bind=engine)
    db_session = DBSession()
    return db_session


def main():
    sb = ScriptBoilerplate(
        module_name=__name__,
        module_file=__file__,
        cwd=os.getcwd(),
        parser_desc="Run tests on the `job_data.sqlite` database.",
        parser_formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    sb.parse_args()
    global logger
    logger = sb.get_logger()
    logger.info("Starting the tests")
    # Read YAML configuration file
    try:
        logger.info("Loading the YAML configuration file '{}'".format(
            sb.args.main_cfg))
        main_cfg = read_yaml_config(sb.args.main_cfg)
    except OSError as e:
        logger.exception(e)
        logger.error("Configuration file '{}' couldn't be read. Program will "
                     "exit.".format(sb.args.main_cfg))
        sys.exit(1)
    else:
        logger.info("Successfully loaded the YAML configuration file")
    global db_session
    db_session = get_db_session(main_cfg)
    for test_name, test_value in main_cfg['tests'].items():
        if test_name == 'industries' and test_value:
            logger.info("Running tests on `industries` ...")
            run_tests_on_industries(db_session, sb.logging_cfg_dict)
        elif test_name == 'job_benefits' and test_value:
            logger.info("Running tests on `job_benefits` ...")
            run_tests_on_job_benefits(db_session, sb.logging_cfg_dict)
        elif test_name == 'job_locations' and test_value:
            logger.info("Running tests on `job_locations` ...")
            run_tests_on_job_locations(db_session, sb.logging_cfg_dict)
        elif test_name == 'job_salaries' and test_value:
            logger.info("Running tests on `job_salaries` ...")
            run_tests_on_job_salaries(db_session, sb.logging_cfg_dict)
        elif test_name == 'roles' and test_value:
            logger.info("Running tests on `roles` ...")
            run_tests_on_roles(db_session, sb.logging_cfg_dict)
        elif test_name == 'skills' and test_value:
            logger.info("Running tests on `skills` ...")
            run_tests_on_skills(db_session, sb.logging_cfg_dict)


if __name__ == '__main__':
    try:
        main()
    except (KeyboardInterrupt, KeyError) as e:
        pass
