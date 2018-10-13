import os
# Third-party modules
import ipdb
# Own modules
from utilities.logging_boilerplate import LoggingBoilerplate

logger = None
db_session = None


def select_job_benefits():
    """
    Returns all records from the `job_benefits` table. A list of tuples is
    returned where a tuple is of the form (job_post_id, name).

    :return: list of tuples of the form (job_post_id, name)
    """
    sql = "SELECT job_post_id, name from job_benefits"
    return db_session.execute(sql).fetchall()


def run_tests_on_job_benefits(db_session_, logging_cfg):
    lb = LoggingBoilerplate(
        module_name=__name__,
        module_file=__file__,
        cwd=os.getcwd(),
        logging_cfg=logging_cfg)
    global logger
    logger = lb.get_logger()
    global db_session
    db_session = db_session_
    logger.info("Retrieving all records from the `job_benefits` table")
    job_benefits = select_job_benefits()
    logger.info("Retrieved {} records".format(len(job_benefits)))
    job_benefits_dict = {}
    report = {'duplicate_job_benefits': {}}
    logger.info("Validating job benefits ...")
    for job_post_id, name in job_benefits:
        job_benefits_dict.setdefault(job_post_id, [])
        if name in job_benefits_dict[job_post_id]:
            logger.warning(
                "Duplicate job benefit '{}' found for {}".format(
                 name, job_post_id))
            report['duplicate_job_benefits'].setdefault(job_post_id, [])
            report['duplicate_job_benefits'][job_post_id].append(name)
        else:
            job_benefits_dict[job_post_id].append(name)
    logger.info("End of tests on the `job_benefits` table")
