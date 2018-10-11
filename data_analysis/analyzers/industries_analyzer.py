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
                'duplicates': [],
                'items': {
                    'labels': ['industry', 'count_desc'],
                    'data': [],
                    'number_of_items': None,
                },
                'job_posts_ids': [],
                'number_of_job_posts': None,
                'published_dates': [],
            },
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
        self.stats["sorted_industries_count"] = industries_count
        self.logger.debug("There are {} distinct industries".format(
                          len(industries_count)))
        self.logger.debug("There are {} occurrences of industries in job "
                          "posts".format(sum(j for i, j in industries_count)))
        barh_cfg = self.main_cfg["industries"]["barh_chart_industries"]
        self._generate_barh_chart(
            barh_type='barh_chart_industries',
            sorted_topic_count=np.array(self.stats["sorted_industries_count"]),
            barh_chart_cfg=barh_cfg)
        if self.main_cfg['industries']['save_report']:
            self._save_report(self.main_cfg['industries']['report_filename'])

    def _count_industries(self):
        """
        Returns industries sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (industry, count).

        :return: list of tuples of the form (industry, count)
        """
        # Old SQL command
        """
        sql = "SELECT name, COUNT(name) as CountOf from industries " \
              "GROUP BY name ORDER BY CountOf DESC"
        return self.db_session.execute(sql).fetchall()
        """
        self.logger.debug("Counting all industries")
        sql = "SELECT job_post_id, name FROM industries"
        results = self.db_session.execute(sql).fetchall()
        results, list_ids, duplicates, skipped = self._count_items(results)
        # Update report for industries
        self._update_graph_report(
            graph_report=self.report['barh'],
            items=results,
            job_post_ids=list_ids,
            duplicates=duplicates,
            skipped=skipped)
        return results
