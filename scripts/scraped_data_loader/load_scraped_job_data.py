import argparse
import glob
import os
import sys
# Third-party modules
import ipdb
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
# Own modules
from tables import Base
from utility.genutil import load_pickle, read_yaml_config
from utility.script_boilerplate import ScriptBoilerplate


def main():
    sb = ScriptBoilerplate(
        module_name=__name__,
        module_file=__file__,
        cwd=os.getcwd(),
        parser_desc="Load scraped job data from pickle files into a database.",
        parser_formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    sb.parse_args()
    logger = sb.get_logger()
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

    # SQLAlchemy database setup
    logger.info("Database setup")
    # Create tables
    db_url = main_cfg['db_url']
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
    logger.info("Loading the scraped job data as pickle files")
    data_dirpath = os.path.expanduser(main_cfg['scraped_job_data_dirpath'])
    list_job_data_filepaths = glob.glob(os.path.join(data_dirpath, "*.pkl"))
    logger.info("There are {} pickle files in '../{}/'".format(
        len(list_job_data_filepaths), os.path.basename(data_dirpath)))
    for i, job_data_filepath in enumerate(list_job_data_filepaths, start=1):
        try:
            logger.info("#{} Loading the pickle file '{}'".format(
                i, os.path.basename(job_data_filepath)))
            scraped_job_data = load_pickle(job_data_filepath)
        except FileNotFoundError as e:
            logger.exception(e)
            logger.error("Scraped job data from '{}' could not be loaded. "
                         "Program will exit.".format(
                          os.path.basename(job_data_filepath)))
            sys.exit(1)
        else:
            logger.info("Finished loading the scraped job data from '{}'".format(
                        job_data_filepath))
        # Load the scraped job data into the database
        logger.info("Loading the scraped job data '{}' into the database".format(
                    os.path.basename(job_data_filepath)))
        for j, (job_post_id, scraping_session) in \
                enumerate(scraped_job_data.items(), start=1):
            try:
                logger.info("#{} Adding job data for job_post_id={}".format(
                            j, job_post_id))
                db_session.add(scraping_session.data.company)
                db_session.commit()
            except IntegrityError as e:
                # Possible cause #1: UNIQUE constraint failed
                # Example: adding a `job_post` with a `job_posts.id` already taken
                # Possible cause #2: NOT NULL constraint failed
                # Example: adding a `job_post` without an URL which is mandatory as
                # specified in the schema
                logger.exception(e)
                db_session.rollback()
            else:
                logger.debug("Successfully added job data for "
                             "job_post_id={}".format(job_post_id))
    ipdb.set_trace()


if __name__ == '__main__':
    try:
        main()
    except (KeyboardInterrupt, KeyError) as e:
        pass
