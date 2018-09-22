import argparse
import logging
import os
import sys
# Third-party modules
import ipdb
# Own modules
# TODO: module path insertion is hardcoded
sys.path.insert(
    0, os.path.expanduser("~/PycharmProjects/github_projects/dev_jobs_insights"))
from data_analysis import job_data_analyzer as ja
# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
from utility.script_boilerplate import ScriptBoilerplate


if __name__ == '__main__':
    sb = ScriptBoilerplate(
        module_name=__name__,
        module_file=__file__,
        cwd=os.getcwd(),
        parser_desc="Run data analysis of Stackoverflow job posts.",
        parser_formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    logger = sb.get_logger()
    try:
        logger.info("Starting the job data analysis")
        ja.JobDataAnalyzer(
            main_config_path=sb.args.main_config,
            logging_config=sb.logging_config_dict).run_analysis()
    except KeyboardInterrupt as e:
        logger.critical(e)
        sys.exit(1)
