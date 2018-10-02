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


logger = None


# Data cleanup: choose the most informative location between
# similar locations
# Case 1: a location only has a country and this country is
# already found in another location
# e.g. 'None, None, US' and 'San Franciso, CA, US'
# Case 2: two very similar cities
# e.g. 'Hlavní msto Praha', 'Hlavní město Praha'
# Case 3: two locations have the same city and country but one has
# a region too
# e.g. 'Toronto, ON, CA' and 'Toronto, CA'
def cleanup_job_locations(job_locations):
    logger.debug("Job locations to be checked: {}".format(
        [l.__str__() for l in job_locations]))
    if len(job_locations) > 1:
        case2_locs_list = []
        case3_locs_dict = {}
        locations_countries = set([l.country for l in job_locations])
        for loc in job_locations:
            if loc.city is None and loc.region is None \
                    and loc.country is not None \
                    and loc.country in locations_countries:
                # Case 1
                logger.warning(
                    "CASE 1: The location '{}' only has the country as not "
                    "'None' and there is already a previously saved location "
                    "with the same country. This location will be skipped since "
                    "it doesn't give more information than the other "
                    "locations.".format(loc.country))
                continue
            ignore = False
            if case2_locs_list:
                for prev_loc in case2_locs_list:
                    # Case 2
                    prev_set = set([i for i in prev_loc.__str__()])
                    cur_set = set([i for i in loc.__str__()])
                    len_interstion = len(prev_set.intersection(cur_set))
                    cur_loc_count_none = loc.__str__().count("None")
                    prev_loc_count_none = case3_locs_dict[key][1]
                    if cur_loc_count_none == prev_loc_count_none and \
                            len_interstion / len(cur_set) > 0.9:
                        if len(cur_set) > len(prev_set):
                            logger.warning(
                                "CASE 2: The current location '{}' is VERY "
                                "similar to another location '{}' and the "
                                "current one has more letters. Therefore, the "
                                "current location '{}' will be kept and the "
                                "previous one will be skipped.".format(
                                 loc, prev_loc, loc))
                            case2_locs_list.remove(prev_loc)
                            prev_key = prev_loc.city + prev_loc.country
                            del case3_locs_dict[prev_key]
                        else:
                            logger.warning(
                                "CASE 2: The current location '{}' is VERY "
                                "similar to another location '{}' but the "
                                "current one has less letters. Therefore, the "
                                "current location '{}' will be skipped.".format(
                                 loc, prev_loc, loc))
                            ignore = True
                        break
            if ignore:
                continue
            else:
                case2_locs_list.append(loc)
            key = "{}{}".format(loc.city, loc.country)
            case3_locs_dict.setdefault(key, None)
            if case3_locs_dict[key]:
                # Case 3
                cur_loc_count_none = loc.__str__().count("None")
                prev_loc_count_none = case3_locs_dict[key][1]
                if cur_loc_count_none < prev_loc_count_none:
                    case3_locs_dict[key] = \
                        (loc, loc.__str__().count("None"))
                    logger.warning(
                        "CASE 3: The location '{}' will be replaced with the "
                        "location '{}' because the latter gives more information "
                        "than the former.".format(
                         loc.__str_(), case3_locs_dict[key][0].__str__()))
            else:
                case3_locs_dict[key] = (loc, loc.__str__().count("None"))
        return [v[0] for v in case3_locs_dict.values()]
    else:
        logger.debug("Only one location found. Thus, no data cleanup.")
        return job_locations


def main():
    sb = ScriptBoilerplate(
        module_name=__name__,
        module_file=__file__,
        cwd=os.getcwd(),
        parser_desc="Load scraped job data from pickle files into a database.",
        parser_formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    sb.parse_args()
    global logger
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
                # IMPORTANT: if I only update
                # `scraping_session.data.job_locations`, the cleanup job locations
                # are not reflected in the database. I need to also update
                # `scraping_session.data.job_post.job_locations`.
                # TODO: check proper way of updating a record before inserting it
                scraping_session.data.job_locations = cleanup_job_locations(
                    scraping_session.data.job_locations)
                scraping_session.data.job_post.job_locations = \
                    scraping_session.data.job_locations
                logger.debug("Job locations to be added: {}".format(
                    [l.__str__() for l in scraping_session.data.job_locations]))
                if len(scraping_session.data.job_locations) > 1:
                    logger.warning("There are {} locations".format(
                        len(scraping_session.data.job_locations)))
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


if __name__ == '__main__':
    try:
        main()
    except (KeyboardInterrupt, KeyError) as e:
        pass
