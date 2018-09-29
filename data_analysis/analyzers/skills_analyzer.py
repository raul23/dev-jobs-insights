import os
# Third-party modules
import ipdb
import numpy as np
# Own modules
from .analyzer import Analyzer


class SkillsAnalyzer(Analyzer):
    def __init__(self, analysis_type, conn, db_session, main_cfg, logging_cfg):
        # Skills stats to compute
        self.stats_names = ["sorted_skills_count"]
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
        # Reset all skills stats to be computed
        self.reset_stats()
        # Get counts of skills, i.e. for each skill we want to know its number of
        # occurrences in job posts
        skills_count = self._count_skills()
        self.logger.debug("There are {} distinct skills".format(
            len(skills_count)))
        self.logger.debug("There are {} occurrences of skills in job "
                          "posts".format(sum(j for i, j in skills_count)))
        # NOTE: these are all the skills and they are sorted in order of
        # decreasing number of occurrences (i.e. most popular skill at first)
        self.stats["sorted_skills_count"] = skills_count
        barh_cfg = self.main_cfg["skills"]["barh_chart_skills"]
        self._generate_barh_chart(
            barh_type='barh_chart_skills',
            sorted_topic_count=np.array(self.stats["sorted_skills_count"]),
            barh_chart_cfg=barh_cfg)

    def _count_skills(self):
        """
        Returns skills sorted in decreasing order of their occurrences in job
        posts. A list of tuples is returned where a tuple is of the form
        (skill, count).

        :return: list of tuples of the form (skill, count)
        """
        sql = "SELECT name, COUNT(name) as CountOf FROM skills GROUP BY " \
              "name ORDER BY CountOf DESC"
        return self.db_session.execute(sql).fetchall()
