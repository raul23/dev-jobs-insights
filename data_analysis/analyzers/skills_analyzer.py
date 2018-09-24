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


class SkillsAnalyzer(Analyzer):
    def __init__(self, conn, db_session, main_config, logging_config):
        # Skills stats to compute
        self.stats_names = ["sorted_skills_count"]
        super().__init__(conn, db_session, main_config, logging_config,
                         self.stats_names)
        sb = LoggingBoilerplate(
            module_name=__name__,
            module_file=__file__,
            cwd=os.getcwd(),
            logging_config=logging_config)
        self.logger = sb.get_logger()

    def run_analysis(self):
        # Reset all skills stats to be computed
        self.reset_stats()
        # Get counts of skills, i.e. for each skill we want to know its number of
        # occurrences in job posts
        skills_count = self._count_skills()
        self.logger.debug(
            "There are {} distinct skills".format(len(skills_count)))
        self.logger.debug("There are in total {} skills".format(
            sum(j for i, j in skills_count)))
        # NOTE: these are all the skills and they are sorted in order of
        # decreasing number of occurrences (i.e. most popular skill at first)
        self.stats["sorted_skills_count"] = np.array(skills_count)
        self._generate_graphs()

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

    # Generate bar chart of skills vs number of job posts
    def _generate_graphs(self):
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        self.logger.info("loading module 'utility.graphutil' ...")
        from utility.graphutil import generate_bar_chart
        self.logger.debug("finished loading module 'utility.graphutil'")
        self.logger.info("Generating bar chart: skills vs number of job posts ...")
        sorted_skills_count = self.stats["sorted_skills_count"]
        bar_chart_skills = self.main_config["graphs_config"]["bar_chart_skills"]
        top_k = bar_chart_skills["top_k"]
        # TODO: place number (of job posts) on top of each bar
        generate_bar_chart(
            x=sorted_skills_count[:top_k, 0],
            y=sorted_skills_count[:top_k, 1].astype(np.int32),
            xlabel=bar_chart_skills["xlabel"],
            ylabel=bar_chart_skills["ylabel"],
            title=bar_chart_skills["title"].format(top_k))
