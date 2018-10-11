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
        self.report = {
            'barh': {
                'duplicates': [],
                'items': {
                    'labels': ['skill', 'count_desc'],
                    'data': [],
                    'number_of_items': None,
                },
                'job_posts_ids': [],
                'number_of_job_posts': None,
                'published_dates': [],  # min, max published dates
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
        # Reset all skills stats to be computed
        self.reset_stats()
        # Get counts of skills, i.e. for each skill we want to know its number of
        # occurrences in job posts
        # NOTE: these are all the skills and they are sorted in order of
        # decreasing number of occurrences (i.e. most popular skill at first)
        skills_count = self._count_skills()
        self.stats["sorted_skills_count"] = skills_count
        self.logger.debug("There are {} distinct skills".format(
            len(skills_count)))
        self.logger.debug("There are {} occurrences of skills in job "
                          "posts".format(sum(j for i, j in skills_count)))
        barh_cfg = self.main_cfg["skills"]["barh_chart_skills"]
        self._generate_barh_chart(
            barh_type='barh_chart_skills',
            sorted_topic_count=np.array(self.stats["sorted_skills_count"]),
            barh_chart_cfg=barh_cfg)
        if self.main_cfg['skills']['save_report']:
            self._save_report(self.main_cfg['skills']['report_filename'])

    def _count_skills(self):
        """
        Returns skills sorted in decreasing order of their occurrences in job
        posts. A list of tuples is returned where a tuple is of the form
        (skill, count).

        :return: list of tuples of the form (skill, count)
        """
        # Old SQL command
        """
        sql = "SELECT name, COUNT(name) as CountOf FROM skills GROUP BY " \
              "name ORDER BY CountOf DESC"
        return self.db_session.execute(sql).fetchall()
        """
        self.logger.debug("Counting all skills")
        sql = "SELECT job_post_id, name FROM skills"
        results = self.db_session.execute(sql).fetchall()
        results, list_ids, duplicates, skipped = self._count_items(results)
        # Update report for skills
        self._update_graph_report(
            graph_report=self.report['barh'],
            items=results,
            job_post_ids=list_ids,
            duplicates=duplicates,
            skipped=skipped
        )
        return results
