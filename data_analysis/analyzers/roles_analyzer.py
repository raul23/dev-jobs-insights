import os
# Third-party modules
import ipdb
import numpy as np
# Own modules
from .analyzer import Analyzer


class RolesAnalyzer(Analyzer):
    def __init__(self, analysis_type, conn, db_session, main_cfg, logging_cfg):
        # Roles stats to compute
        self.stats_names = ["sorted_roles_count"]
        self.report = {
            'barh': {
                'duplicates': [],
                'items': {
                    'labels': ['role', 'count_desc'],
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
        # Reset all roles stats to be computed
        self.reset_stats()
        # Get counts of roles, i.e. for each role we want to know its number of
        # occurrences in job posts
        # NOTE: these are all the roles and they are sorted in order of decreasing
        # number of occurrences (i.e. most popular role at first)
        roles_count = self._count_roles()
        self.stats["sorted_roles_count"] = roles_count
        self.logger.debug("There are {} distinct roles".format(len(roles_count)))
        self.logger.debug("There are {} occurrences of roles in job "
                          "posts".format(sum(j for i, j in roles_count)))
        barh_cfg = self.main_cfg["roles"]["barh_chart_roles"]
        self._generate_barh_chart(
            barh_type='barh_chart_roles',
            sorted_topic_count=np.array(self.stats["sorted_roles_count"]),
            barh_chart_cfg=barh_cfg)
        if self.main_cfg['roles']['save_report']:
            self._save_report(self.main_cfg['roles']['report_filename'])

    def _count_roles(self):
        """
        Returns roles sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (name, count).

        :return: list of tuples of the form (name, count)
        """
        # Old SQL command
        """
        sql = "SELECT name, COUNT(name) as CountOf FROM roles GROUP " \
              "BY name ORDER BY CountOf DESC"
        return self.db_session.execute(sql).fetchall()
        """
        self.logger.debug("Counting all roles")
        sql = "SELECT job_post_id, name FROM roles"
        results = self.db_session.execute(sql).fetchall()
        results, list_ids, duplicates, skipped = self._count_items(results)
        # Update report for roles
        self._update_graph_report(
            graph_report=self.report['barh'],
            items=results,
            job_post_ids=list_ids,
            duplicates=duplicates,
            skipped=skipped
        )
        return results
