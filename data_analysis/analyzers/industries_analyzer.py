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
        # Reset all industries stats to be computed
        self.reset_stats()
        # Get number of job posts for each industry
        # NOTE: the returned results are sorted in decreasing order of
        # industries' count, i.e. from the most popular industry to the least
        # popular industry
        self._clean_industries_names()
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

    def _clean_industries_names(self):
        # Standardize the names of the industries
        # NOTE: only the most obvious industries names are standardized. The
        # other less obvious ones are left intact, e.g. 'IT Consulting' could
        # be renamed to 'Consulting' but 'Consulting' is a too broad category
        # and you might lose information doing so.
        # Same for 'Advertising Technology' and 'Advertising'.
        # NOTE: Typos are also fixed
        # NOTE: Some industries should not even be considered as industries
        # e.g. JavaScript, functional programming, facebook, iOS
        # 'and Compliance' seems to be an incomplete name for an industry
        industry_names = {
            'Software Development / Engineering': 'Software Development',
            'eCommerce': 'E-Commerce',
            'Retail - eCommerce': 'E-Commerce',
            'Health Care': 'Healthcare',
            'Fasion': 'Fashion',
            'fintech': 'Financial Technology',
            'blockchain': 'Blockchain',
            'higher': 'Higher Education'
        }
        self.logger.info("Cleaning names of industries")
        for old_name, new_name in industry_names.items():
            sql = "UPDATE industries SET name='{1}' WHERE name='{0}'".format(
                old_name, new_name)
            result = self.db_session.execute(sql)
            self.db_session.commit()
            if result.rowcount > 0:
                self.logger.info(
                    "The industry name '{0}' was changed to '{1}': {2} "
                    "time{3}".format(
                        old_name, new_name, result.rowcount,
                        's' if result.rowcount > 1 else ''))
            else:
                self.logger.warning(
                    "The industry name '{0}' couldn't be found".format(old_name))

    def _count_industries(self):
        """
        Returns industries sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (industry, count).

        :return: list of tuples of the form (industry, count)
        """
        sql = "SELECT name, COUNT(name) as CountOf from industries " \
              "GROUP BY name ORDER BY CountOf DESC"
        return self.db_session.execute(sql).fetchall()
