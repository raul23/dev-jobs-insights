import os
import sys
# Third-party modules
import ipdb
import numpy as np
# Own modules
from .analyzer import Analyzer
# TODO: module path insertion is hardcoded
sys.path.insert(0, os.path.expanduser("~/PycharmProjects/github_projects"))
from utility.script_boilerplate import LoggingBoilerplate


class JobBenefitsAnalyzer(Analyzer):
    def __init__(self, conn, db_session, main_config, logging_config):
        # Job benefits stats to compute
        self.stats_names = ["sorted_job_benefits_count"]
        super().__init__(conn, db_session, main_config, logging_config,
                         self.stats_names)
        sb = LoggingBoilerplate(
            module_name=__name__,
            module_file=__file__,
            cwd=os.getcwd(),
            logging_config=logging_config)
        self.logger = sb.get_logger()

    def run_analysis(self):
        # Reset all job benefits stats to be computed
        self.reset_stats()
        # Get counts of job benefits, i.e. for each job benefit we want to know
        # its number of occurrences in job posts
        job_benefits_count = self._count_job_benefits()
        self.logger.debug(
            "There are {} distinct job benefits".format(len(job_benefits_count)))
        self.logger.debug("There are in total {} job benefits".format(
            sum(j for i, j in job_benefits_count)))
        # NOTE: these are all the job benefits and they are sorted in order of
        # decreasing number of occurrences (i.e. most popular job benefit at
        # first)
        self.stats["sorted_job_benefits_count"] = np.array(job_benefits_count)
        self._generate_graphs()

    def _count_job_benefits(self):
        """
        Returns job benefits sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (job_benefit, count).

        :return: list of tuples of the form (job_benefit, count)
        """
        sql = "SELECT name, COUNT(name) as CountOf FROM job_benefits GROUP " \
              "BY name ORDER BY CountOf DESC"
        return self.db_session.execute(sql).fetchall()

    # Generate bar chart of job benefits vs number of job posts
    def _generate_graphs(self):
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        self.logger.info("loading module 'utility.graphutil' ...")
        from utility.graphutil import generate_bar_chart
        self.logger.debug("finished loading module 'utility.graphutil'")
        self.logger.info(
            "Generating bar chart: job benefits vs number of job posts ...")
        sorted_job_benefits_count = self.stats["sorted_job_benefits_count"]
        bar_chart_job_benefits = \
            self.main_config["graphs_config"]["bar_chart_job_benefits"]
        top_k = bar_chart_job_benefits["top_k"]
        # TODO: place number (of job posts) on top of each bar
        generate_bar_chart(
            # x=self._shrink_labels(sorted_job_benefits_count[:top_k, 0], self.main_config["graphs_config"][""]),
            x=sorted_job_benefits_count[:top_k, 0],
            y=sorted_job_benefits_count[:top_k, 1].astype(np.int32),
            xlabel=bar_chart_job_benefits["xlabel"],
            ylabel=bar_chart_job_benefits["ylabel"],
            title=bar_chart_job_benefits["title"].format(top_k))
