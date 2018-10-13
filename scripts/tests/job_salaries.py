import os
# Third-party modules
import ipdb
# Own modules
from utilities.logging_boilerplate import LoggingBoilerplate

logger = None
db_session = None


def select_job_salaries():
    """
    Returns all records from the `job_salaries` table. A list of tuples is
    returned where a tuple is of the form
    (job_post_id, min_salary, max_salary, currency, conversion_time).

    :return: list of tuples of the form
             (job_post_id, min_salary, max_salary, currency, conversion_time)
    """
    sql = "SELECT job_post_id, min_salary, max_salary, currency, " \
          "conversion_time from job_salaries"
    return db_session.execute(sql).fetchall()


def run_tests_on_job_salaries(db_session_, logging_cfg):
    lb = LoggingBoilerplate(
        module_name=__name__,
        module_file=__file__,
        cwd=os.getcwd(),
        logging_cfg=logging_cfg)
    global logger
    logger = lb.get_logger()
    global db_session
    db_session = db_session_
    logger.info("Retrieving all records from the `job_salaries` table")
    salaries = select_job_salaries()
    logger.info("Retrieved {} records".format(len(salaries)))
    salaries_dict = {}
    rates_dict = {}
    currencies = set()
    logger.info("Test 1: Validating conversion of currency and conversion time")
    for job_post_id, min_salary, max_salary, currency, conversion_time \
            in salaries:
        salaries_dict.setdefault(job_post_id, None)
        currencies.add(currency)
        if conversion_time:
            assert salaries_dict[job_post_id] is not None, \
                "A `job_post_id` should already have been processed if " \
                "`conversion_time` is not 'None'"
            orig_currency, count = salaries_dict[job_post_id]
            assert count == 1, \
                "One job_post_id should at most be associated with two rows"
            assert orig_currency != 'USD', \
                "A USD salary can't be converted to another currency"
            salaries_dict[job_post_id][1] += 1
            key_rates = "{}_{}".format(orig_currency, currency)
            rates_dict.setdefault(
                "{}_{}".format(orig_currency, currency), conversion_time)
            assert conversion_time == rates_dict[key_rates], \
                "The current conversion time ('{}') for the rate '{}' is " \
                "different from the expected one '{}'".format(
                 conversion_time, key_rates, rates_dict[key_rates])
        else:
            assert salaries_dict[job_post_id] is None, \
                "A `job_post_id` should not already have been processed if " \
                "`conversion_time` is 'None'"
            salaries_dict[job_post_id] = [currency, 1]
    logger.info("Test 1 passed!")
    logger.info("These are the {} different currencies: {}".format(
                 len(currencies), currencies))
    logger.info("End of tests on the `job_salaries` table")
