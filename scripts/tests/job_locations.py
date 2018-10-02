import os
# Third-party modules
import ipdb
# Own modules
from analyzers.job_locations_analyzer import build_location
from utility.logging_boilerplate import LoggingBoilerplate

logger = None
db_session = None


def select_job_locations():
    """
    Returns all records from the `job_locations` table. A list of tuples is
    returned where a tuple is of the form (job_post_id, city, region, country).

    :return: list of tuples of the form (job_post_id, city, region, country)
    """
    sql = "SELECT job_post_id, city, region, country from job_locations"
    return db_session.execute(sql).fetchall()


def run_tests_on_job_locations(db_session_, logging_cfg):
    lb = LoggingBoilerplate(
        module_name=__name__,
        module_file=__file__,
        cwd=os.getcwd(),
        logging_cfg=logging_cfg)
    global logger
    logger = lb.get_logger()
    global db_session
    db_session = db_session_
    logger.info("Retrieving all records from the `job_locations` table")
    locations = select_job_locations()
    logger.info("Retrieved {} records".format(len(locations)))
    locations_dict = {}
    report = {'duplicate_job_locations': {}}
    logger.info("Validating job locations ...")
    for job_post_id, city, region, country in locations:
        locations_dict.setdefault(job_post_id, [])
        location = build_location([city, region, country])
        if location in locations_dict[job_post_id]:
            logger.warning(
                "Duplicate location '{}' found for {}".format(location, job_post_id))
            report['duplicate_job_locations'].setdefault(job_post_id, [])
            report['duplicate_job_locations'][job_post_id].append(location)
        else:
            locations_dict[job_post_id].append(location)
        if len(locations_dict[job_post_id]) > 1:
            logger.warning(
                "There are more than one location for {}".format(job_post_id))
            logger.info("These are the locations so far: {}".format(locations_dict[job_post_id]))
    logger.info("End of tests on the `job_locations` table")
