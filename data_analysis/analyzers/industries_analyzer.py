import os
# Third-party modules
import ipdb
import numpy as np
# Own modules
from .analyzer import Analyzer


class IndustriesAnalyzer(Analyzer):
    def __init__(self, analysis_type, conn, db_session, main_cfg, logging_cfg):
        # Industries stats to compute
        self.stats_names = ["sorted_industries_count"]
        self.report = {
            'barh': {
                'number_of_job_posts': None,
                'number_of_countries': None,
                'published_dates': [],
                'top_10_countries': [],
                'job_posts_ids': [],
                'duplicates': [],
            }
        }
        super().__init__(analysis_type,
                         conn,
                         db_session,
                         main_cfg,
                         logging_cfg,
                         self.stats_names,
                         self.report,
                         __name__,
                         __file__,
                         os.getcwd())

    def run_analysis(self):
        # Reset all industries stats to be computed
        self.reset_stats()
        # Get number of job posts for each industry
        # NOTE: the returned results are sorted in decreasing order of
        # industries' count, i.e. from the most popular industry to the least
        # popular industry
        industries_count = self._count_industries()
        self.logger.debug(
            "There are {} distinct industries".format(len(industries_count)))
        self.logger.debug(
            "There are {} occurrences of industries in job posts".format(
                sum(j for i, j in industries_count)))
        self.stats["sorted_industries_count"] = industries_count
        barh_cfg = self.main_cfg["industries"]["barh_chart_industries"]
        self._generate_barh_chart(
            barh_type='barh_chart_industries',
            sorted_topic_count=np.array(self.stats["sorted_industries_count"]),
            barh_chart_cfg=barh_cfg)

    def _count_industries(self):
        """
        Returns industries sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (name, count).

        :return: list of tuples of the form (name, count)
        """
        sql = "SELECT name, COUNT(name) as CountOf from industries " \
              "GROUP BY name ORDER BY CountOf DESC"
        return self.db_session.execute(sql).fetchall()
