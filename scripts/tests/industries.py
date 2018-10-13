import os
# Third-party modules
import ipdb
# Own modules
from utilities.logging_boilerplate import LoggingBoilerplate

logger = None
db_session = None


def select_industries():
    """
    Returns all records from the `industries` table. A list of tuples is returned
    where a tuple is of the form (job_post_id, name).

    :return: list of tuples of the form (job_post_id, name)
    """
    sql = "SELECT job_post_id, name from industries"
    return db_session.execute(sql).fetchall()


def run_tests_on_industries(db_session_, logging_cfg):
    lb = LoggingBoilerplate(
        module_name=__name__,
        module_file=__file__,
        cwd=os.getcwd(),
        logging_cfg=logging_cfg)
    global logger
    logger = lb.get_logger()
    global db_session
    db_session = db_session_
    logger.info("Retrieving all records from the `industries` table")
    industries = select_industries()
    logger.info("Retrieved {} records".format(len(industries)))
    industries_dict = {}
    report = {'duplicate_industries': {}}
    logger.info("Validating industries ...")
    for job_post_id, name in industries:
        industries_dict.setdefault(job_post_id, [])
        if name in industries_dict[job_post_id]:
            logger.warning(
                "Duplicate industry '{}' found for {}".format(name, job_post_id))
            report['duplicate_industries'].setdefault(job_post_id, [])
            report['duplicate_industries'][job_post_id].append(name)
        else:
            industries_dict[job_post_id].append(name)
    logger.info("End of tests on the `industries` table")
    # Group by `job_post_id`
    # Count industries per `job_post_id`
    # Count duplicate industries per `job_post_id`
