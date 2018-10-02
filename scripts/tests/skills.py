import os
# Third-party modules
import ipdb
# Own modules
from utility.logging_boilerplate import LoggingBoilerplate

logger = None
db_session = None


def select_skills():
    """
    Returns all records from the `skills` table. A list of tuples is returned
    where a tuple is of the form (job_post_id, name).

    :return: list of tuples of the form (job_post_id, name)
    """
    sql = "SELECT job_post_id, name from skills"
    return db_session.execute(sql).fetchall()


def run_tests_on_skills(db_session_, logging_cfg):
    lb = LoggingBoilerplate(
        module_name=__name__,
        module_file=__file__,
        cwd=os.getcwd(),
        logging_cfg=logging_cfg)
    global logger
    logger = lb.get_logger()
    global db_session
    db_session = db_session_
    logger.info("Retrieving all records from the `skils` table")
    skills = select_skills()
    logger.info("Retrieved {} records".format(len(skills)))
    skills_dict = {}
    report = {'duplicate_skills': {}}
    logger.info("Validating skills ...")
    for job_post_id, name in skills:
        skills_dict.setdefault(job_post_id, [])
        if name in skills_dict[job_post_id]:
            logger.warning(
                "Duplicate skill '{}' found for {}".format(name, job_post_id))
            report['duplicate_skills'].setdefault(job_post_id, [])
            report['duplicate_skills'][job_post_id].append(name)
        else:
            skills_dict[job_post_id].append(name)
    logger.info("End of tests on the `skills` table")
