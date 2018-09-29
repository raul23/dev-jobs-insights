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
        # Reset all roles stats to be computed
        self.reset_stats()
        # Get counts of roles, i.e. for each role we want to know its number of
        # occurrences in job posts
        roles_count = self._count_roles()
        self.logger.debug("There are {} distinct roles".format(len(roles_count)))
        self.logger.debug("There are {} occurrences of roles in job "
                          "posts".format(sum(j for i, j in roles_count)))
        # NOTE: these are all the roles and they are sorted in order of
        # decreasing number of occurrences (i.e. most popular role at
        # first)
        self.stats["sorted_roles_count"] = roles_count
        barh_cfg = self.main_cfg["roles"]["barh_chart_roles"]
        self._generate_barh_chart(
            barh_type='barh_chart_roles',
            sorted_topic_count=np.array(self.stats["sorted_roles_count"]),
            barh_chart_cfg=barh_cfg)

    def _count_roles(self):
        """
        Returns roles sorted in decreasing order of their occurrences in
        job posts. A list of tuples is returned where a tuple is of the form
        (role, count).

        :return: list of tuples of the form (role, count)
        """
        sql = "SELECT name, COUNT(name) as CountOf FROM roles GROUP " \
              "BY name ORDER BY CountOf DESC"
        return self.db_session.execute(sql).fetchall()
