import numpy as np

from .abstract_analyzer import AbstractAnalyzer
from utility import util, graph_util as g_util


class TagAnalyzer(AbstractAnalyzer):
    def __init__(self, conn, config_ini):
        # Tags stats to compute
        self.stats_names = ["sorted_tags_count"]
        super().__init__(conn, config_ini, self.stats_names)

    def run_analysis(self):
        # Reset all tag stats to be computed
        self.reset_stats()
        # Get counts of tags, i.e. for each tag we want to know its number of
        # occurrences in job posts
        results = self.count_tag_occurrences()
        # NOTE: these are all the tags (even those that don't have a salary
        # associated with) and they are sorted in order of decreasing
        # number of occurrences (i.e. most popular tag at first)
        self.stats["sorted_tags_count"] = np.array(results)
        self.generate_graphs()

    def count_tag_occurrences(self):
        """
        Returns tags sorted in decreasing order of their occurrences in job posts.
        A list of tuples is returned where a tuple is of the form (tag_name, count).

        :return: list of tuples of the form (tag_name, count)
        """
        sql = '''SELECT name, COUNT(name) as CountOf FROM entries_tags GROUP BY name ORDER BY CountOf DESC'''
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def generate_graphs(self):
        # Generate bar chart of tags vs number of job posts
        top_k = self.config_ini["bar_chart_tags"]["top_k"]
        config = {"x": self.stats["sorted_tags_count"][:top_k, 0],
                  "y": self.stats["sorted_tags_count"][:top_k, 1].astype(np.int32),
                  "xlabel": self.config_ini["bar_chart_tags"]["xlabel"],
                  "ylabel": self.config_ini["bar_chart_tags"]["ylabel"],
                  "title": self.config_ini["bar_chart_tags"]["title"],
                  "grid_which": self.config_ini["bar_chart_tags"]["grid_which"]}
        # TODO: place number (of job posts) on top of each bar
        g_util.generate_bar_chart(config)