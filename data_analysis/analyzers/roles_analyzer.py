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
        self.logger.debug(
            "There are {} distinct roles".format(len(roles_count)))
        self.logger.debug("There are in total {} roles".format(
            sum(j for i, j in roles_count)))
        # NOTE: these are all the roles and they are sorted in order of
        # decreasing number of occurrences (i.e. most popular role at
        # first)
        self.stats["sorted_roles_count"] = np.array(roles_count)
        self._generate_graphs()

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

    # Generate bar chart of roles vs number of job posts
    def _generate_graphs(self):
        # Lazy import. Loading of module takes lots of time. So do it only when
        # needed
        self.logger.info("loading module 'utility.graphutil' ...")
        from utility.graphutil import generate_bar_chart
        self.logger.debug("finished loading module 'utility.graphutil'")
        self.logger.info(
            "Generating bar chart: roles vs number of job posts ...")
        sorted_roles_count = self.stats["sorted_roles_count"]
        bar_chart_roles = \
            self.main_config["graphs_config"]["bar_chart_roles"]
        top_k = bar_chart_roles["top_k"]
        new_labels = self._shrink_labels(
            labels=sorted_roles_count[:top_k, 0],
            max_length=bar_chart_roles["max_xtick_label_length"])
        generate_bar_chart(
            x=np.array(new_labels),
            y=sorted_roles_count[:top_k, 1].astype(np.int32),
            xlabel=bar_chart_roles["xlabel"],
            ylabel=bar_chart_roles["ylabel"],
            title=bar_chart_roles["title"].format(top_k),
            grid_which=bar_chart_roles["grid_which"],
            fig_width=bar_chart_roles["fig_width"],
            fig_height=bar_chart_roles["fig_height"])
