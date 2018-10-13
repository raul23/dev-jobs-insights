import os
# Third-party modules
import ipdb
# Own modules
from utilities.logging_boilerplate import LoggingBoilerplate

logger = None
db_session = None


def select_roles():
    """
    Returns all records from the `roles` table. A list of tuples is returned
    where a tuple is of the form (job_post_id, name).

    :return: list of tuples of the form (job_post_id, name)
    """
    sql = "SELECT job_post_id, name from roles"
    return db_session.execute(sql).fetchall()


def run_tests_on_roles(db_session_, logging_cfg):
    lb = LoggingBoilerplate(
        module_name=__name__,
        module_file=__file__,
        cwd=os.getcwd(),
        logging_cfg=logging_cfg)
    global logger
    logger = lb.get_logger()
    global db_session
    db_session = db_session_
    logger.info("Retrieving all records from the `roles` table")
    roles = select_roles()
    logger.info("Retrieved {} records".format(len(roles)))
    roles_dict = {}
    report = {'duplicate_roles': {}}
    logger.info("Validating roles ...")
    for job_post_id, name in roles:
        roles_dict.setdefault(job_post_id, [])
        if name in roles_dict[job_post_id]:
            logger.warning(
                "Duplicate role '{}' found for {}".format(name, job_post_id))
            report['duplicate_roles'].setdefault(job_post_id, [])
            report['duplicate_roles'][job_post_id].append(name)
        else:
            roles_dict[job_post_id].append(name)
    logger.info("End of tests on the `roles` table")
