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


class RolesAnalyzer(Analyzer):
    def __init__(self, conn, db_session, main_config, logging_config):
        # Roles stats to compute
        self.stats_names = ["sorted_roles_count"]
        super().__init__(conn, db_session, main_config, logging_config,
                         self.stats_names)
        sb = LoggingBoilerplate(
            module_name=__name__,
            module_file=__file__,
            cwd=os.getcwd(),
            logging_config=logging_config)
        self.logger = sb.get_logger()

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
        bar_config = self.main_config["graphs_config"]["bar_chart_roles"]
        self._generate_bar_chart(
            sorted_topic_count=self.stats["sorted_roles_count"],
            bar_chart_config=bar_config)

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

    def _generate_bar_chart(self, sorted_topic_count, bar_chart_config):
        sorted_topic_count = np.array(sorted_topic_count)
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        # TODO: add spinner when loading this module
        self.logger.info("loading module 'utility.graphutil' ...")
        from utility.graphutil import draw_bar_chart
        self.logger.debug("finished loading module 'utility.graphutil'")
        self.logger.info(
            "Generating bar chart: {} vs {} ...".format(
                bar_chart_config['xlabel'], bar_chart_config['ylabel']))
        topk = bar_chart_config['topk']
        new_labels = self._shrink_labels(
            labels=sorted_topic_count[:topk, 0],
            max_length=bar_chart_config['max_xtick_label_length'])
        draw_bar_chart(
            x=np.array(new_labels),
            y=sorted_topic_count[:topk, 1].astype(np.int32),
            xlabel=bar_chart_config['xlabel'],
            ylabel=bar_chart_config['ylabel'],
            title=bar_chart_config['title'].format(topk),
            grid_which=bar_chart_config['grid_which'],
            color=bar_chart_config['color'],
            fig_width=bar_chart_config['fig_width'],
            fig_height=bar_chart_config['fig_height'])
