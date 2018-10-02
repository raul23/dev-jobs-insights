import argparse
import os
import sys
# Third-party modules
import ipdb
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
import numpy as np
# Own modules
from utility.genutil import read_yaml_config
from utility.script_boilerplate import ScriptBoilerplate


def get_db_session(self):
    # SQLAlchemy database setup
    db_url = self.main_cfg['db_url']
    db_url['database'] = os.path.expanduser(db_url['database'])
    self.logger.info("Database setup of {}".format(db_url['database']))
    engine = create_engine(URL(**db_url))
    Base.metadata.bind = engine
    # Setup database session
    DBSession = sessionmaker(bind=engine)
    db_session = DBSession()
    return db_session


def main():
    ipdb.set_trace()
    sb = ScriptBoilerplate(
        module_name=__name__,
        module_file=__file__,
        cwd=os.getcwd(),
        parser_desc="Run tests on the `industries` table.",
        parser_formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    sb.parse_args()
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
    ipdb.set_trace()

if __name__ == '__main__':
    if __name__ == '__main__':
        try:
            main()
        except (KeyboardInterrupt, KeyError) as e:
            pass
