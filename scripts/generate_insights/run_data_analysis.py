import os
import sys
# Third-party modules
import ipdb
# Own modules
# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects/dev_jobs_insights"))
from data_analysis import job_analyzer as ja


if __name__ == '__main__':
    data_analyzer = ja.JobDataAnalyzer(config_path="config.yaml")
    data_analyzer.run_analysis()