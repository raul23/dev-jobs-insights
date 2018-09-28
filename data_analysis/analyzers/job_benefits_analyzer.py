import os
import sys
# Third-party modules
import ipdb
import numpy as np
# Own modules
from .analyzer import Analyzer


class JobBenefitsAnalyzer(Analyzer):
    def __init__(self, analysis_type, conn, db_session, main_cfg, logging_cfg):
        # Job benefits stats to compute
        self.stats_names = ["sorted_job_benefits_count"]
        super().__init__(analysis_type,
                         conn,
                         db_session,
                         main_cfg,
                         logging_cfg,
                         self.stats_names,
                         __name__,
                         __file__,
                         os.getcwd())

    def run_analysis(self):
        # Reset all job benefits stats to be computed
        self.reset_stats()
        # Get counts of job benefits, i.e. for each job benefit we want to know
        # its number of occurrences in job posts
        job_benefits_count = self._count_job_benefits()
        self.logger.debug("There are {} distinct job benefits".format(
            len(job_benefits_count)))
        self.logger.debug("There are {} occurrences of job benefits in job "
                          "posts".format(sum(j for i, j in job_benefits_count)))
        # NOTE: these are all the job benefits and they are sorted in order of
        # decreasing number of occurrences (i.e. most popular job benefit at
        # first)
        self.stats["sorted_job_benefits_count"] = job_benefits_count
        barh_cfg = self.main_cfg["job_benefits"]["barh_chart_job_benefits"]
        self._generate_barh_chart(
            sorted_topic_count=np.array(self.stats["sorted_job_benefits_count"]),
            barh_chart_cfg=barh_cfg)

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
