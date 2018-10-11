import os
# Third-party modules
import ipdb
import numpy as np
# Own modules
from .analyzer import Analyzer


class JobBenefitsAnalyzer(Analyzer):
    def __init__(self, analysis_type, conn, db_session, main_cfg, logging_cfg):
        # Job benefits stats to compute
        self.stats_names = ["sorted_job_benefits_count"]
        self.report = {
            'barh': {
                'duplicates': [],
                'items': {
                    'labels': ['job_benefit', 'count_desc'],
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
        # Reset all job benefits stats to be computed
        self.reset_stats()
        # Get counts of job benefits, i.e. for each job benefit we want to know
        # its number of occurrences in job posts
        # NOTE: Job benfits are sorted in order of decreasing number of
        # occurrences (i.e. most popular job benefit at first)
        job_benefits_count = self._count_job_benefits()
        self.stats["sorted_job_benefits_count"] = job_benefits_count
        self.logger.debug("There are {} distinct job benefits".format(
            len(job_benefits_count)))
        self.logger.debug("There are {} occurrences of job benefits in job "
                          "posts".format(sum(j for i, j in job_benefits_count)))
        barh_cfg = self.main_cfg["job_benefits"]["barh_chart_job_benefits"]
        self._generate_barh_chart(
            barh_type='barh_chart_job_benefits',
            sorted_topic_count=np.array(self.stats["sorted_job_benefits_count"]),
            barh_chart_cfg=barh_cfg)
        if self.main_cfg['job_benefits']['save_report']:
            self._save_report(self.main_cfg['job_benefits']['report_filename'])

    def _count_job_benefits(self):
        """
        Returns job benefits sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (job_benefit, count).

        :return: list of tuples of the form (job_benefit, count)
        """
        # Old SQL command
        """
        sql = "SELECT name, COUNT(name) as CountOf FROM job_benefits GROUP " \
              "BY name ORDER BY CountOf DESC"
        return self.db_session.execute(sql).fetchall()
        """
        self.logger.debug("Counting all job benefits")
        sql = "SELECT job_post_id, name FROM job_benefits"
        results = self.db_session.execute(sql).fetchall()
        results, list_ids, duplicates, skipped = self._count_items(results)
        # Update report for benefits
        self._update_graph_report(
            graph_report=self.report['barh'],
            items=results,
            job_post_ids=list_ids,
            duplicates=duplicates,
            skipped=skipped
        )
        return results
